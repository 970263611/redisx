package com.dahuaboke.redisx;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.common.cache.CacheMonitor;
import com.dahuaboke.redisx.common.enums.FlushState;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.thread.RedisxThreadFactory;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.dahuaboke.redisx.console.ConsoleServer;
import com.dahuaboke.redisx.from.FromClient;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.ClusterInfoHandler;
import com.dahuaboke.redisx.handler.SentinelInfoHandler;
import com.dahuaboke.redisx.to.ToClient;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * 2024/6/13 14:02
 * auth: dahua
 * desc: 全局控制器
 */
public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private static ScheduledExecutorService controllerPool = Executors.newScheduledThreadPool(1, new RedisxThreadFactory(Constants.PROJECT_NAME + "-Controller"));
    private Mode fromMode;
    private Mode toMode;
    private CacheManager cacheManager;
    private CacheMonitor cacheMonitor;
    private boolean immediate;
    private int immediateResendTimes;
    private String switchFlag;
    private List<InetSocketAddress> fromNodeAddresses;
    private List<InetSocketAddress> toNodeAddresses;
    private String fromMasterName;
    private String toMasterName;
    private boolean startConsole;
    private int consolePort;
    private int consoleTimeout;
    private boolean alwaysFullSync;
    private boolean syncRdb;
    private int toFlushSize;
    private boolean flushDb;
    private boolean syncWithCheckSlot;
    private boolean verticalScaling;
    private boolean connectFromMaster;

    public Controller(Redisx.Config config) {
        this.fromNodeAddresses = config.getFromAddresses();
        this.toNodeAddresses = config.getToAddresses();
        this.immediate = config.isImmediate();
        this.immediateResendTimes = config.getImmediateResendTimes();
        this.switchFlag = config.getSwitchFlag();
        this.fromMode = config.getFromMode();
        this.toMode = config.getToMode();
        this.fromMasterName = config.getFromMasterName();
        this.toMasterName = config.getToMasterName();
        this.startConsole = config.isConsoleEnable();
        this.consolePort = config.getConsolePort();
        this.consoleTimeout = config.getConsoleTimeout();
        this.alwaysFullSync = config.isAlwaysFullSync();
        this.syncRdb = config.isSyncRdb();
        this.toFlushSize = config.getToFlushSize();
        this.flushDb = config.isFlushDb();
        this.syncWithCheckSlot = config.isSyncWithCheckSlot();
        this.verticalScaling = config.isVerticalScaling();
        this.connectFromMaster = config.isConnectMaster();
        cacheManager = new CacheManager(config.getRedisVersion(), fromMode, config.getFromPassword(), toMode, config.getToPassword());
        if (flushDb) {
            cacheManager.setFlushState(FlushState.PREPARE);
        }
        if (startConsole) {
            cacheMonitor = new CacheMonitor(cacheManager);
            cacheMonitor.setConfig(config);
        }
    }

    public void start() {
        logger.info("Application global id is {}", cacheManager.getId());
        //关闭netty内存监测，不准
        System.setProperty("io.netty.leakDetection.level", "disabled");
        //注册shutdownhook
        registerShutdownKook();
        //需要确保上一次执行结束再执行下一次任务
        controllerPool.scheduleAtFixedRate(() -> {
            try {
                boolean isMaster = cacheManager.isMaster();
                boolean fromIsStarted = cacheManager.fromIsStarted();
                List<Context> allContexts = cacheManager.getAllContexts();
                if (cacheManager.toIsStarted()) {
                    if (FlushState.BEGINNING == cacheManager.getFlushState()) {
                        flushAllTo(allContexts);
                    } else {
                        preemptMasterAndMonitor(allContexts);
                    }
                } else {
                    //识别to集群中存在节点宕机则全部关闭to
                    isMaster = false;
                    cacheManager.closeAllTo();
                }
                if (isMaster && !fromIsStarted) { //抢占到主节点，from未启动
                    logger.info("Upgrade master and starting from clients");
                    startAllFrom(alwaysFullSync, syncRdb);
                    cacheManager.setFromIsStarted(true);
                } else if (isMaster && fromIsStarted) { //抢占到主节点，from已经启动
                    //do nothing
                    logger.trace("State: master");
                } else if (!isMaster && fromIsStarted) { //未抢占到主节点，from已经启动
                    logger.info("Downgrade slave and closing from clients");
                    cacheManager.closeAllFrom();
                    cacheManager.setFromIsStarted(false);
                } else if (!isMaster && !fromIsStarted) { //未抢占到主节点，from未启动
                    if (!cacheManager.toIsStarted()) {
                        startAllTo(toFlushSize, flushDb, syncWithCheckSlot);
                    }
                } else {
                    //bug do nothing
                    logger.warn("Unknown application state");
                }
                //控制台相关
                if (startConsole) {
                    if (cacheManager.toIsStarted() && cacheManager.fromIsStarted()) {
                        if (cacheManager.getConsoleContext() == null) {
                            ConsoleNode consoleNode = new ConsoleNode("localhost", consolePort, consoleTimeout);
                            Context context = consoleNode.getContext();
                            cacheManager.setConsoleContext((ConsoleContext) context);
                            consoleNode.start();
                        }
                    } else {
                        ConsoleContext consoleContext = cacheManager.getConsoleContext();
                        if (consoleContext != null) {
                            consoleContext.close();
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Controller schedule error {}", e);
            }
        }, 5000, 1000000, TimeUnit.MICROSECONDS); //用微秒减少主从抢占脑裂问题，纳秒个人感觉太夸张了
    }

    public Executor getExecutor(String name) {
        RedisxThreadFactory defaultThreadFactory = new RedisxThreadFactory(Constants.PROJECT_NAME + "-" + name);
        return new ThreadPerTaskExecutor(defaultThreadFactory);
    }

    private void closeLog4jShutdownHook() {
        LoggerContextFactory factory = LogManager.getFactory();
        if (factory instanceof Log4jContextFactory) {
            Log4jContextFactory contextFactory = (Log4jContextFactory) factory;
            ((DefaultShutdownCallbackRegistry) contextFactory.getShutdownCallbackRegistry()).stop();
        }
    }

    private void flushAllTo(List<Context> allContexts) {
        cacheManager.setFlushState(FlushState.END);
        for (Context cont : allContexts) {
            if (cont instanceof ToContext) {
                ToContext toContext = (ToContext) cont;
                if (toContext.isAdapt(toMode, switchFlag)) {
                    toContext.preemptMasterAndFlushAll();
                } else {
                    toContext.sendCommand(Constants.FLUSH_ALL_COMMAND, 1000, true, false);
                }
            }
        }
    }

    private void preemptMasterAndMonitor(List<Context> allContexts) {
        for (Context cont : allContexts) {
            if (cont instanceof FromContext) {
                FromContext fromContext = (FromContext) cont;
                if (startConsole) {
                    fromContext.putWriteCount();
                }
            } else if (cont instanceof ToContext) {
                ToContext toContext = (ToContext) cont;
                if (toContext.isAdapt(toMode, switchFlag)) {
                    //如果本身是主节点则同时写入偏移量
                    toContext.preemptMaster();
                }
                if (startConsole) {
                    toContext.putWriteCount();
                }
            }
        }
    }

    private void startAllFrom(boolean alwaysFullSync, boolean syncRdb) {
        List<InetSocketAddress> fromMasterNodesInfo = getFromMasterNodesInfo();
        if (fromMasterNodesInfo == null) {
            logger.error("[From] master nodes info can not get");
            return;
        }
        fromMasterNodesInfo.forEach(address -> {
            String host = address.getHostString();
            int port = address.getPort();
            FromNode fromNode = new FromNode("Sync", cacheManager, host, port, startConsole, false, alwaysFullSync, syncRdb, false);
            fromNode.start();
            if (fromNode.isStarted(5000)) {
                cacheManager.register(fromNode.getContext());
            }
        });
    }

    private void startAllTo(int toFlushSize, boolean flushDb, boolean syncWithCheckSlot) {
        List<InetSocketAddress> toMasterNodesInfo = getToMasterNodesInfo(syncWithCheckSlot);
        if (toMasterNodesInfo == null) {
            logger.error("[To] master nodes info can not get");
            return;
        }
        boolean success = true;
        for (InetSocketAddress address : toMasterNodesInfo) {
            String host = address.getHostString();
            int port = address.getPort();
            ToNode toNode = new ToNode("Sync", cacheManager, host, port, toMode, startConsole, false, immediate, immediateResendTimes, switchFlag, toFlushSize, false, flushDb);
            toNode.start();
            if (toNode.isStarted(5000)) {
                Context context = toNode.getContext();
                if (context == null) {
                    logger.error("[{}:{}] context is null", host, port);
                }
                cacheManager.register(context);
            } else {
                logger.error("[{}:{}] node start failed, close all [to] node", host, port);
                success = false;
                break;
            }
        }
        cacheManager.setToStarted(success);
    }

    private void registerShutdownKook() {
        Thread shutdownHookThread = new Thread(() -> {
            logger.info("Shutdown hook thread is starting");
            controllerPool.shutdown();
            logger.info("Update offset thread shutdown");
            cacheManager.closeAllFrom();
            List<Context> allContexts = cacheManager.getAllContexts();
            while (true) {
                boolean allowClose = true;
                Iterator<Context> iterator = allContexts.iterator();
                while (iterator.hasNext()) {
                    Context cont = iterator.next();
                    if (cont instanceof ToContext) {
                        ToContext toContext = (ToContext) cont;
                        if (!toContext.isClose) {
                            if (!cacheManager.checkHasNeedWriteCommand(toContext)) {
                                iterator.remove();
                                toContext.close();
                            }
                            allowClose = false;
                            break;
                        }
                    }
                }
                if (allowClose) {
                    break;
                }
            }
            logger.info("Application exit success");
            //否则日志不一定打印，因为shutdownHook顺序无法保证
            LogManager.shutdown();
        });
        shutdownHookThread.setName(Constants.PROJECT_NAME + "-ShutdownHook");
        if (!immediate) {
            closeLog4jShutdownHook();
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
    }

    public List<InetSocketAddress> getFromMasterNodesInfo() {
        if (Mode.CLUSTER == fromMode) {
            cacheManager.clearFromNodesInfo();
        }
        if (Mode.CLUSTER == fromMode || Mode.SENTINEL == fromMode) {
            for (InetSocketAddress address : fromNodeAddresses) {
                String host = address.getHostString();
                int port = address.getPort();
                FromNode fromNode = new FromNode("GetFromNodesInfo", cacheManager, host, port, startConsole, false, false, false, true);
                fromNode.start();
                if (fromNode.isStarted(5000)) {
                    FromContext context = (FromContext) fromNode.getContext();
                    try {
                        if (context == null) {
                            logger.error("[{}:{}] context is null", host, port);
                        }
                        if (context.nodesInfoGetSuccess(5000)) {
                            break;
                        }
                    } finally {
                        context.close();
                    }
                } else {
                    logger.error("[{}:{}] nodes info get failed", host, port);
                }
            }
        }
        if (Mode.CLUSTER == fromMode) {
            if (cacheManager.getFromClusterNodesInfo().isEmpty()) {
                return null;
            }
            List<InetSocketAddress> addresses = new ArrayList<>();
            if (!verticalScaling) {
                fromNodeAddresses.clear();
                List<ClusterInfoHandler.SlotInfo> masterSlotInfoList = new ArrayList<>();
                for (ClusterInfoHandler.SlotInfo slotInfo : cacheManager.getFromClusterNodesInfo()) {
                    fromNodeAddresses.add(new InetSocketAddress(slotInfo.getIp(), slotInfo.getPort()));
                    if (slotInfo.isActiveMaster()) {
                        masterSlotInfoList.add(slotInfo);
                        if (connectFromMaster) {
                            addresses.add(new InetSocketAddress(slotInfo.getIp(), slotInfo.getPort()));
                        }
                    }
                }
                if (!connectFromMaster) {
                    for (ClusterInfoHandler.SlotInfo masterInfo : masterSlotInfoList) {
                        String id = masterInfo.getId();
                        boolean getInfoSuccess = false;
                        for (ClusterInfoHandler.SlotInfo slaveInfo : cacheManager.getFromClusterNodesInfo()) {
                            if (id.equals(slaveInfo.getMasterId()) && slaveInfo.isActive()) {
                                addresses.add(new InetSocketAddress(slaveInfo.getIp(), slaveInfo.getPort()));
                                getInfoSuccess = true;
                                break;
                            }
                        }
                        if (!getInfoSuccess) {
                            addresses.add(new InetSocketAddress(masterInfo.getIp(), masterInfo.getPort()));
                        }
                    }
                }
            } else {
                for (InetSocketAddress fromNodeAddress : fromNodeAddresses) {
                    boolean select = false;
                    String fromHost = fromNodeAddress.getHostString();
                    int fromPort = fromNodeAddress.getPort();
                    for (ClusterInfoHandler.SlotInfo slotInfo : cacheManager.getFromClusterNodesInfo()) {
                        String host = slotInfo.getIp();
                        int port = slotInfo.getPort();
                        if (fromHost.equals(host) && fromPort == port) {
                            if (!slotInfo.isActive()) {
                                logger.error("Select node need active, but down [{}:{}]", fromHost, fromPort);
                                return null;
                            }
                            select = true;
                        }
                    }
                    if (!select) {
                        logger.error("Select node need in cluster [{}:{}]", fromHost, fromPort);
                        return null;
                    }
                }
                addresses.addAll(fromNodeAddresses);
            }
            return addresses;
        } else if (Mode.SENTINEL == fromMode) {
            List<InetSocketAddress> addresses = new ArrayList<>();
            for (SentinelInfoHandler.SlaveInfo slaveInfo : cacheManager.getFromSentinelNodesInfo()) {
                if (slaveInfo.isActive()) {
                    if (connectFromMaster) {
                        addresses.add(new InetSocketAddress(slaveInfo.getMasterHost(), slaveInfo.getMasterPort()));
                    } else {
                        addresses.add(new InetSocketAddress(slaveInfo.getIp(), slaveInfo.getPort()));
                    }
                    break;
                }
            }
            if (addresses.isEmpty()) {
                return null;
            }
            return addresses;
        }
        return fromNodeAddresses;
    }

    public List<InetSocketAddress> getToMasterNodesInfo(boolean syncWithCheckSlot) {
        if (Mode.CLUSTER == toMode) {
            cacheManager.clearToNodesInfo();
        }
        if (Mode.CLUSTER == toMode || Mode.SENTINEL == toMode) {
            for (InetSocketAddress address : toNodeAddresses) {
                String host = address.getHostString();
                int port = address.getPort();
                ToNode toNode = new ToNode("GetToNodesInfo", cacheManager, host, port, toMode, startConsole, false, false, 0, switchFlag, 0, true, false);
                toNode.start();
                if (toNode.isStarted(5000)) {
                    ToContext context = (ToContext) toNode.getContext();
                    try {
                        if (context == null) {
                            logger.error("[{}:{}] context is null", host, port);
                        }
                        if (context.nodesInfoGetSuccess(5000)) {
                            break;
                        }
                    } finally {
                        context.close();
                    }
                } else {
                    logger.error("[{}:{}] nodes info get failed", host, port);
                }
            }
        }
        if (Mode.CLUSTER == toMode) {
            if (cacheManager.getToClusterNodesInfo().isEmpty()) {
                return null;
            }
            toNodeAddresses.clear();
            List<ClusterInfoHandler.SlotInfo> masterSlotInfoList = new ArrayList<>();
            List<InetSocketAddress> addresses = new ArrayList<>();
            for (ClusterInfoHandler.SlotInfo slotInfo : cacheManager.getToClusterNodesInfo()) {
                toNodeAddresses.add(new InetSocketAddress(slotInfo.getIp(), slotInfo.getPort()));
                if (slotInfo.isActiveMaster()) {
                    masterSlotInfoList.add(slotInfo);
                    addresses.add(new InetSocketAddress(slotInfo.getIp(), slotInfo.getPort()));
                }
            }
            if (syncWithCheckSlot) {
                int start = 0;
                boolean checkComplete = false;
                while (!checkComplete) {
                    boolean right = false;
                    for (ClusterInfoHandler.SlotInfo slotInfo : masterSlotInfoList) {
                        if (start == (16383 + 1)) {
                            checkComplete = true;
                            right = true;
                            break;
                        }
                        if (slotInfo.getSlotStart() == start) {
                            start = slotInfo.getSlotEnd() + 1;
                            right = true;
                            break;
                        }
                    }
                    if (!right) {
                        return null;
                    }
                }
            }
            return addresses;
        } else if (Mode.SENTINEL == toMode) {
            List<InetSocketAddress> addresses = new ArrayList<>();
            InetSocketAddress toSentinelMaster = cacheManager.getToSentinelMaster();
            if (toSentinelMaster == null) {
                return null;
            }
            addresses.add(toSentinelMaster);
            return addresses;
        }
        return toNodeAddresses;
    }

    /**
     * Node 部分
     */
    private abstract class Node extends Thread {

        protected String name;

        protected CountDownLatch flag = new CountDownLatch(1);

        abstract String nodeName();

        abstract Context getContext();

        public boolean isStarted(int timeout) {
            try {
                return flag.await(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return false;
            }
        }
    }

    private class ToNode extends Node {
        private CacheManager cacheManager;
        private String host;
        private int port;
        private ToContext toContext;

        public ToNode(String threadNamePrefix, CacheManager cacheManager, String host, int port, Mode toMode, boolean startConsole, boolean startByConsole, boolean immediate, int immediateResendTimes, String switchFlag, int flushSize, boolean isNodesInfoContext, boolean flushDb) {
            this.name = Constants.PROJECT_NAME + "-" + threadNamePrefix + "-ToNode-" + host + "-" + port;
            this.setName(name);
            this.cacheManager = cacheManager;
            this.host = host;
            this.port = port;
            //放在构造方法而不是run，因为兼容console模式，需要收集context，否则可能收集到null
            this.toContext = new ToContext(cacheManager, host, port, fromMode, toMode, startConsole, startByConsole, immediate, immediateResendTimes, switchFlag, flushSize, isNodesInfoContext, flushDb, toMasterName);
        }

        @Override
        public void run() {
            cacheManager.registerTo(this.toContext);
            ToClient toClient = new ToClient(this.toContext, getExecutor("ToEventLoop-" + host + ":" + port));
            this.toContext.setClient(toClient);
            toClient.start(flag);
        }

        @Override
        public String nodeName() {
            return name;
        }

        @Override
        public Context getContext() {
            return toContext;
        }
    }

    private class FromNode extends Node {
        private String host;
        private int port;
        private FromContext fromContext;

        public FromNode(String threadNamePrefix, CacheManager cacheManager, String host, int port, boolean startConsole, boolean startByConsole, boolean alwaysFullSync, boolean syncRdb, boolean isNodesInfoContext) {
            this.name = Constants.PROJECT_NAME + "-" + threadNamePrefix + "-FromNode - " + host + " - " + port;
            this.setName(name);
            this.host = host;
            this.port = port;
            //放在构造方法而不是run，因为兼容console模式，需要收集console，否则可能收集到null
            this.fromContext = new FromContext(cacheManager, host, port, startConsole, startByConsole, fromMode, toMode, alwaysFullSync, syncRdb, isNodesInfoContext, fromMasterName, connectFromMaster);
        }

        @Override
        public void run() {
            FromClient fromClient = new FromClient(this.fromContext, getExecutor("FromEventLoop-" + host + ":" + port));
            this.fromContext.setFromClient(fromClient);
            fromClient.start(flag);
        }

        @Override
        public String nodeName() {
            return name;
        }

        @Override
        public Context getContext() {
            return fromContext;
        }
    }

    private class ConsoleNode extends Node {
        private String host;
        private int port;
        private int timeout;
        private ConsoleContext consoleContext;

        public ConsoleNode(String host, int port, int timeout) {
            this.name = "Console[" + host + ":" + port + "]";
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            consoleContext = new ConsoleContext(cacheManager, cacheMonitor, this.host, this.port, this.timeout, toMode, fromMode);
        }

        @Override
        public void run() {
            getFromMasterNodesInfo().forEach(address -> {
                String host = address.getHostString();
                int port = address.getPort();
                FromNode fromNode = new FromNode("Console", cacheManager, host, port, startConsole, true, false, false, false);
                consoleContext.setFromContext((FromContext) fromNode.getContext());
                fromNode.start();
            });
            getToMasterNodesInfo(false).forEach(address -> {
                String host = address.getHostString();
                int port = address.getPort();
                ToNode toNode = new ToNode("Console", cacheManager, host, port, toMode, startConsole, true, immediate, 0, switchFlag, 0, false, false);
                consoleContext.setToContext((ToContext) toNode.getContext());
                toNode.start();
            });
            ConsoleServer consoleServer = new ConsoleServer(consoleContext, getExecutor("Console-Boss"), getExecutor("Console-Worker"));
            consoleServer.start();
            consoleContext.setConsoleServer(consoleServer);
            flag.countDown();
        }

        @Override
        public String nodeName() {
            return name;
        }

        @Override
        public Context getContext() {
            return consoleContext;
        }
    }
}

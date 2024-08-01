package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.dahuaboke.redisx.console.ConsoleServer;
import com.dahuaboke.redisx.from.FromClient;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.thread.RedisxThreadFactory;
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
import java.util.List;
import java.util.concurrent.*;

/**
 * 2024/6/13 14:02
 * auth: dahua
 * desc: 全局控制器
 */
public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private static ScheduledExecutorService controllerPool = Executors.newScheduledThreadPool(1, new RedisxThreadFactory(Constant.PROJECT_NAME + "-Controller"));
    private boolean toIsCluster;
    private boolean fromIsCluster;
    private CacheManager cacheManager;
    private boolean immediate;
    private int immediateResendTimes;
    private String switchFlag;

    public Controller(String redisVersion, boolean fromIsCluster, String fromPassword, boolean toIsCluster, String toPassword, boolean immediate, int immediateResendTimes, String switchFlag) {
        this.toIsCluster = toIsCluster;
        this.fromIsCluster = fromIsCluster;
        this.immediate = immediate;
        this.immediateResendTimes = immediateResendTimes;
        this.switchFlag = switchFlag;
        cacheManager = new CacheManager(redisVersion, fromIsCluster, fromPassword, toIsCluster, toPassword);
    }

    public void start(List<InetSocketAddress> fromNodeAddresses, List<InetSocketAddress> toNodeAddresses, boolean startConsole, int consolePort, int consoleTimeout, boolean alwaysFullSync, boolean syncRdb, int toFlushSize) {
        closeLog4jShutdownHook();
        logger.info("Application global id is {}", cacheManager.getId());
        Thread shutdownHookThread = new Thread(() -> {
            logger.info("Shutdown hook thread is starting");
            controllerPool.shutdown();
            logger.info("Update offset thread shutdown");
            cacheManager.closeAllFrom();
            List<Context> allContexts = cacheManager.getAllContexts();
            while (true) {
                boolean allowClose = true;
                for (Context cont : allContexts) {
                    if (cont instanceof ToContext) {
                        ToContext toContext = (ToContext) cont;
                        if (!toContext.isClose) {
                            if (!cacheManager.checkHasNeedWriteCommand(toContext)) {
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
        shutdownHookThread.setName(Constant.PROJECT_NAME + "-ShutdownHook");
        if (!immediate) {
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        //scheduleWithFixedDelay not scheduleAtFixedRate，无需关心异常
        controllerPool.scheduleAtFixedRate(() -> {
            boolean isMaster = cacheManager.isMaster();
            boolean fromIsStarted = cacheManager.fromIsStarted();
            List<Context> allContexts = cacheManager.getAllContexts();
            if (cacheManager.getToStarted()) {
                for (Context cont : allContexts) {
                    if (cont instanceof ToContext) {
                        ToContext toContext = (ToContext) cont;
                        if (toContext.isAdapt(toIsCluster, switchFlag)) {
                            //如果本身是主节点则同时写入偏移量
                            toContext.preemptMaster();
                        }
                    }
                }
            } else {
                //识别to集群中存在节点宕机则全部关闭to
                isMaster = false;
                cacheManager.closeAllTo();
            }
            if (isMaster && !fromIsStarted) { //抢占到主节点，from未启动
                logger.info("Upgrade master and starting from clients");
                startAllFrom(fromNodeAddresses, alwaysFullSync, syncRdb);
                cacheManager.setFromIsStarted(true);
            } else if (isMaster && fromIsStarted) { //抢占到主节点，from已经启动
                //do nothing
                logger.trace("state: master");
            } else if (!isMaster && fromIsStarted) { //未抢占到主节点，from已经启动
                logger.info("Downgrade slave and closing from clients");
                cacheManager.closeAllFrom();
                cacheManager.setFromIsStarted(false);
            } else if (!isMaster && !fromIsStarted) { //未抢占到主节点，from未启动
                if (!cacheManager.getToStarted()) {
                    startAllTo(toNodeAddresses, toFlushSize);
                }
            } else {
                //bug do nothing
                logger.warn("Unknown application state");
            }
        }, 5000, 1000000, TimeUnit.MICROSECONDS); //用微秒减少主从抢占脑裂问题，纳秒个人感觉太夸张了
        if (startConsole) {
            new ConsoleNode("localhost", consolePort, consoleTimeout, toNodeAddresses, fromNodeAddresses).start();
        }
    }

    public Executor getExecutor(String name) {
        RedisxThreadFactory defaultThreadFactory = new RedisxThreadFactory(Constant.PROJECT_NAME + "-" + name);
        return new ThreadPerTaskExecutor(defaultThreadFactory);
    }

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

        public ToNode(String threadNamePrefix, CacheManager cacheManager, String host, int port, boolean toIsCluster, boolean isConsole, boolean immediate, int immediateResendTimes, String switchFlag, int flushSize) {
            this.name = Constant.PROJECT_NAME + "-" + threadNamePrefix + "-ToNode-" + host + "-" + port;
            this.setName(name);
            this.cacheManager = cacheManager;
            this.host = host;
            this.port = port;
            //放在构造方法而不是run，因为兼容console模式，需要收集context，否则可能收集到null
            this.toContext = new ToContext(cacheManager, host, port, fromIsCluster, toIsCluster, isConsole, immediate, immediateResendTimes, switchFlag, flushSize);
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

        public FromNode(String threadNamePrefix, CacheManager cacheManager, String host, int port, boolean isConsole, boolean alwaysFullSync, boolean syncRdb) {
            this.name = Constant.PROJECT_NAME + "-" + threadNamePrefix + "-FromNode - " + host + " - " + port;
            this.setName(name);
            this.host = host;
            this.port = port;
            //放在构造方法而不是run，因为兼容console模式，需要收集console，否则可能收集到null
            this.fromContext = new FromContext(cacheManager, host, port, isConsole, fromIsCluster, toIsCluster, alwaysFullSync, syncRdb);
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
        private List<InetSocketAddress> toNodeAddresses;
        private List<InetSocketAddress> fromNodeAddresses;
        private ConsoleContext consoleContext;

        public ConsoleNode(String host, int port, int timeout, List<InetSocketAddress> toNodeAddresses, List<InetSocketAddress> fromNodeAddresses) {
            this.name = "Console[" + host + ":" + port + "]";
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            this.toNodeAddresses = toNodeAddresses;
            this.fromNodeAddresses = fromNodeAddresses;
            consoleContext = new ConsoleContext(this.host, this.port, this.timeout, toIsCluster, fromIsCluster);
        }

        @Override
        public void run() {
            toNodeAddresses.forEach(address -> {
                String host = address.getHostString();
                int port = address.getPort();
                ToNode toNode = new ToNode("Console", cacheManager, host, port, toIsCluster, true, immediate, 0, switchFlag, 0);
                consoleContext.setToContext((ToContext) toNode.getContext());
                toNode.start();
            });
            fromNodeAddresses.forEach(address -> {
                String host = address.getHostString();
                int port = address.getPort();
                FromNode fromNode = new FromNode("Console", cacheManager, host, port, true, false, false);
                consoleContext.setFromContext((FromContext) fromNode.getContext());
                fromNode.start();
            });
            ConsoleServer consoleServer = new ConsoleServer(consoleContext, getExecutor("Console-Boss"), getExecutor("Console-Worker"));
            consoleServer.start();
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

    private void closeLog4jShutdownHook() {
        LoggerContextFactory factory = LogManager.getFactory();
        if (factory instanceof Log4jContextFactory) {
            Log4jContextFactory contextFactory = (Log4jContextFactory) factory;
            ((DefaultShutdownCallbackRegistry) contextFactory.getShutdownCallbackRegistry()).stop();
        }
    }

    private void startAllFrom(List<InetSocketAddress> fromNodeAddresses, boolean alwaysFullSync, boolean syncRdb) {
        fromNodeAddresses.forEach(address -> {
            String host = address.getHostString();
            int port = address.getPort();
            FromNode fromNode = new FromNode("Sync", cacheManager, host, port, false, alwaysFullSync, syncRdb);
            fromNode.start();
            if (fromNode.isStarted(5000)) {
                cacheManager.register(fromNode.getContext());
            }
        });
    }

    private void startAllTo(List<InetSocketAddress> toNodeAddresses, int toFlushSize) {
        boolean success = true;
        for (InetSocketAddress address : toNodeAddresses) {
            String host = address.getHostString();
            int port = address.getPort();
            ToNode toNode = new ToNode("Sync", cacheManager, host, port, toIsCluster, false, immediate, immediateResendTimes, switchFlag, toFlushSize);
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
                cacheManager.closeAllTo();
                break;
            }
        }
        cacheManager.setToStarted(success);
    }
}

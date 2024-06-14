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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 2024/6/13 14:02
 * auth: dahua
 * desc: 全局控制器
 */
public class Controller {

    private static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private static ScheduledExecutorService controllerPool = Executors.newScheduledThreadPool(1,
            new RedisxThreadFactory(Constant.PROJECT_NAME + "-Controller"));
    private boolean toIsCluster;
    private boolean fromIsCluster;
    private CacheManager cacheManager;


    public Controller(boolean toIsCluster, boolean fromIsCluster) {
        this.toIsCluster = toIsCluster;
        this.fromIsCluster = fromIsCluster;
        cacheManager = new CacheManager(toIsCluster, fromIsCluster);
    }

    public void start(List<InetSocketAddress> toNodeAddresses, List<InetSocketAddress> fromNodeAddresses, boolean startConsole,
                      InetSocketAddress consoleAddress, int consoleTimeout) {
        logger.info("Application global id is {}", cacheManager.getId());
        toNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            ToNode toNode = new ToNode("Sync", cacheManager, host, port, toIsCluster, false);
            toNode.start();
            cacheManager.register(toNode.getContext());
        });
        controllerPool.scheduleAtFixedRate(() -> {
            List<Context> allContexts = cacheManager.getAllContexts();
            for (Context cont : allContexts) {
                if (cont instanceof ToContext) {
                    ToContext toContext = (ToContext) cont;
                    if (toContext.isAdapt(true, Constant.DR_KEY)) {
                        toContext.preemptMaster();
                    }
                }
            }
            boolean isMaster = cacheManager.isMaster();
            boolean fromIsStarted = cacheManager.fromIsStarted();
            if (isMaster && !fromIsStarted) { //抢占到主节点，from未启动
                logger.info("Upgrade master and starting from clients");
                fromNodeAddresses.forEach(address -> {
                    String host = address.getHostName();
                    int port = address.getPort();
                    FromNode fromNode = new FromNode("Sync", cacheManager, host, port, false, fromIsCluster);
                    fromNode.start();
                    cacheManager.register(fromNode.getContext());
                });
                cacheManager.setFromIsStarted(true);
            } else if (isMaster && fromIsStarted) { //抢占到主节点，from已经启动
                //do nothing
                logger.debug("Already upgraded master and started from clients");
            } else if (!isMaster && fromIsStarted) { //未抢占到主节点，from已经启动
                logger.info("Downgrade slave and closing from clients");
                for (Context cont : allContexts) {
                    if (cont instanceof FromContext) {
                        FromContext fromContext = (FromContext) cont;
                        fromContext.close();
                    }
                }
            } else if (!isMaster && fromIsStarted) { //未抢占到主节点，from未启动
                //do nothing
                logger.debug("Already slave and closed from clients");
            } else {
                //bug do nothing
                logger.warn("Unknown application state");
            }
        }, 0, 1, TimeUnit.SECONDS);
        if (startConsole) {
            String consoleHost = consoleAddress.getHostName();
            int consolePort = consoleAddress.getPort();
            new ConsoleNode(consoleHost, consolePort, consoleTimeout, toNodeAddresses, fromNodeAddresses).start();
        }
    }

    public Executor getExecutor(String name) {
        RedisxThreadFactory defaultThreadFactory = new RedisxThreadFactory(Constant.PROJECT_NAME + "-" + name);
        return new ThreadPerTaskExecutor(defaultThreadFactory);
    }

    private abstract class Node extends Thread {

        protected String name;

        abstract String nodeName();

        abstract Context getContext();
    }

    private class ToNode extends Node {
        private CacheManager cacheManager;
        private String host;
        private int port;
        private ToContext toContext;

        public ToNode(String threadNamePrefix, CacheManager cacheManager, String host, int port, boolean toIsCluster, boolean isConsole) {
            this.name = Constant.PROJECT_NAME + "-" + threadNamePrefix + "-ToNode-" + host + "-" + port;
            this.setName(name);
            this.cacheManager = cacheManager;
            this.host = host;
            this.port = port;
            //放在构造方法而不是run，因为兼容console模式，需要收集context，否则可能收集到null
            this.toContext = new ToContext(cacheManager, host, port, toIsCluster, isConsole);
        }

        @Override
        public void run() {
            cacheManager.registerTo(this.toContext);
            ToClient toClient = new ToClient(this.toContext,
                    getExecutor("ToEventLoop-" + host + ":" + port));
            this.toContext.setClient(toClient);
            toClient.start();
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

        public FromNode(String threadNamePrefix, CacheManager cacheManager, String host, int port, boolean isConsole, boolean masterIsCluster) {
            this.name = Constant.PROJECT_NAME + "-" + threadNamePrefix + "-FromNode - " + host + " - " + port;
            this.setName(name);
            this.host = host;
            this.port = port;
            //放在构造方法而不是run，因为兼容console模式，需要收集console，否则可能收集到null
            this.fromContext = new FromContext(cacheManager, host, port, isConsole, masterIsCluster);
        }

        @Override
        public void run() {
            FromClient fromClient = new FromClient(this.fromContext, getExecutor("FromEventLoop-" + host + ":" + port));
            this.fromContext.setFromClient(fromClient);
            fromClient.start();
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
                String host = address.getHostName();
                int port = address.getPort();
                ToNode toNode = new ToNode("Console", cacheManager, host, port, toIsCluster, true);
                consoleContext.setToContext((ToContext) toNode.getContext());
                toNode.start();
            });
            fromNodeAddresses.forEach(address -> {
                String host = address.getHostName();
                int port = address.getPort();
                FromNode fromNode = new FromNode("Console", cacheManager, host, port, true, fromIsCluster);
                consoleContext.setFromContext((FromContext) fromNode.getContext());
                fromNode.start();
            });
            ConsoleServer consoleServer = new ConsoleServer(consoleContext, getExecutor("Console-Boss"), getExecutor("Console-Worker"));
            consoleServer.start();
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

package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.dahuaboke.redisx.console.ConsoleServer;
import com.dahuaboke.redisx.from.FromClient;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.SlotInfoHandler;
import com.dahuaboke.redisx.thread.RedisxThreadFactory;
import com.dahuaboke.redisx.to.ToClient;
import com.dahuaboke.redisx.to.ToContext;
import com.dahuaboke.redisx.utils.CRC16;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    private static Lock lock = new ReentrantLock();
    private static boolean monitorStart = false;
    private static Map<String, Node> nodes = new ConcurrentHashMap();
    private static ScheduledExecutorService pool = Executors.newScheduledThreadPool(1,
            new RedisxThreadFactory(Constant.PROJECT_NAME + "-Monitor"));
    private CacheManager cacheManager;
    private boolean toIsCluster;
    private boolean fromIsCluster;
    protected BlockingDeque<String> replyQueue;
    protected SlotInfoHandler.SlotInfo slotInfo;
    protected boolean isClose = false;
    protected boolean isConsole;

    public Context() {
    }

    public Context(boolean toIsCluster, boolean fromIsCluster) {
        cacheManager = new CacheManager(toIsCluster, fromIsCluster);
        this.toIsCluster = toIsCluster;
        this.fromIsCluster = fromIsCluster;
    }

    public void start(List<InetSocketAddress> toNodeAddresses, List<InetSocketAddress> fromNodeAddresses, InetSocketAddress consoleAddress, int consoleTimeout) {
        toNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new ToNode("Sync", cacheManager, host, port, toIsCluster, false).start();
        });
        fromNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new FromNode("Sync", cacheManager, host, port, false, fromIsCluster).start();
        });
        String consoleHost = consoleAddress.getHostName();
        int consolePort = consoleAddress.getPort();
        new ConsoleNode(consoleHost, consolePort, consoleTimeout, toNodeAddresses, fromNodeAddresses).start();
    }

    public boolean isAdapt(boolean isCluster, String command) {
        return false;
    }

    public String sendCommand(String command, int timeout) {
        throw new RuntimeException();
    }

    public void setSlotInfo(SlotInfoHandler.SlotInfo slotInfo) {
        this.slotInfo = slotInfo;
    }

    protected int calculateHash(String command) {
        String[] ary = command.split(" ");
        if (ary.length > 1) {
            return CRC16.crc16(ary[1].getBytes(StandardCharsets.UTF_8));
        } else {
            logger.warn("Command split length should > 1");
            return 0;
        }
    }

    public Executor getExecutor(String name) {
        RedisxThreadFactory defaultThreadFactory = new RedisxThreadFactory(Constant.PROJECT_NAME + "-" + name);
        return new ThreadPerTaskExecutor(defaultThreadFactory);
    }

    public boolean isClose() {
        return isClose;
    }

    public void setClose(boolean close) {
        isClose = close;
    }

    private abstract class Node extends Thread {

        protected String name;

        public Node() {
            try {
                lock.lock();
                if (!monitorStart) {
                    monitorStart = true;
                    pool.scheduleAtFixedRate(() -> {
                        AtomicInteger alive = new AtomicInteger();
                        nodes.forEach((k, v) -> {
                            Context context = v.getContext();
                            if (context != null && context.isClose()) {
                                logger.error("[{}] node down", k);
                            } else {
                                alive.getAndIncrement();
                            }
                        });
                        if (alive.get() == 1) {
                            nodes.forEach((k, v) -> {
                                Context context = v.getContext();
                                if (!context.isClose()) {
                                    if (context instanceof ConsoleContext) {
                                        logger.error("Find only console context live, progress exit");
                                        System.exit(0);
                                    }
                                }
                            });
                        }
                    }, 0, 1, TimeUnit.MINUTES);
                    logger.debug("Monitor thread start");
                }
            } finally {
                lock.unlock();
            }
        }

        protected void register2Monitor(Node node) {
            nodes.put(node.nodeName(), node);
        }

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
            //放在构造方法而不是run，因为兼容console模式，需要收集console，否则可能收集到null
            this.toContext = new ToContext(cacheManager, host, port, toIsCluster, isConsole);
        }

        @Override
        public void run() {
            register2Monitor(this);
            cacheManager.register(this.toContext);
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
            register2Monitor(this);
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
            register2Monitor(this);
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

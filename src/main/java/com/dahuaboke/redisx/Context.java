package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.dahuaboke.redisx.console.ConsoleServer;
import com.dahuaboke.redisx.handler.SlotInfoHandler;
import com.dahuaboke.redisx.forwarder.ForwarderClient;
import com.dahuaboke.redisx.forwarder.ForwarderContext;
import com.dahuaboke.redisx.slave.SlaveClient;
import com.dahuaboke.redisx.slave.SlaveContext;
import com.dahuaboke.redisx.thread.RedisxThreadFactory;
import com.dahuaboke.redisx.utils.CRC16;
import io.netty.util.concurrent.ThreadPerTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    private CacheManager cacheManager;
    private boolean forwarderIsCluster;
    private boolean masterIsCluster;
    protected BlockingDeque<String> replyQueue;
    protected SlotInfoHandler.SlotInfo slotInfo;

    public Context() {
    }

    public Context(boolean forwarderIsCluster, boolean masterIsCluster) {
        cacheManager = new CacheManager(forwarderIsCluster, masterIsCluster);
        this.forwarderIsCluster = forwarderIsCluster;
        this.masterIsCluster = masterIsCluster;
    }

    public void start(List<InetSocketAddress> forwarderNodeAddresses, List<InetSocketAddress> slaveNodeAddresses, InetSocketAddress consoleAddress, int consoleTimeout) {
        forwarderNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new ForwarderNode("MAIN", cacheManager, host, port, forwarderIsCluster, false).start();
        });
        slaveNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new SlaveNode("MAIN", cacheManager, host, port, false, masterIsCluster).start();
        });
        String consoleHost = consoleAddress.getHostName();
        int consolePort = consoleAddress.getPort();
        new ConsoleNode(consoleHost, consolePort, consoleTimeout, forwarderNodeAddresses, slaveNodeAddresses).start();
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

    private class ForwarderNode extends Thread {
        private CacheManager cacheManager;
        private String forwardHost;
        private int forwardPort;
        private ForwarderContext forwarderContext;

        public ForwarderNode(String threadNamePrefix, CacheManager cacheManager, String forwardHost, int forwardPort, boolean forwarderIsCluster, boolean isConsole) {
            this.setName(Constant.PROJECT_NAME + "-" + threadNamePrefix + "-ForwardNode-" + forwardHost + "-" + forwardPort);
            this.cacheManager = cacheManager;
            this.forwardHost = forwardHost;
            this.forwardPort = forwardPort;
            //放在构造方法而不是run，因为兼容console模式，需要收集console，否则可能收集到null
            this.forwarderContext = new ForwarderContext(cacheManager, forwardHost, forwardPort, forwarderIsCluster, isConsole);
        }

        @Override
        public void run() {
            cacheManager.register(this.forwarderContext);
            ForwarderClient forwarderClient = new ForwarderClient(this.forwarderContext,
                    getExecutor("ForwarderEventLoop-" + forwardHost + ":" + forwardPort));
            this.forwarderContext.setForwarderClient(forwarderClient);
            forwarderClient.start();
        }

        public Context getContext() {
            return forwarderContext;
        }
    }

    private class SlaveNode extends Thread {
        private String masterHost;
        private int masterPort;
        private SlaveContext slaveContext;

        public SlaveNode(String threadNamePrefix, CacheManager cacheManager, String masterHost, int masterPort, boolean isConsole, boolean masterIsCluster) {
            this.setName(Constant.PROJECT_NAME + "-" + threadNamePrefix + "-SlaveNode - " + masterHost + " - " + masterPort);
            this.setDaemon(true);
            this.masterHost = masterHost;
            this.masterPort = masterPort;
            //放在构造方法而不是run，因为兼容console模式，需要收集console，否则可能收集到null
            this.slaveContext = new SlaveContext(cacheManager, masterHost, masterPort, isConsole, masterIsCluster);
        }

        @Override
        public void run() {
            SlaveClient slaveClient = new SlaveClient(this.slaveContext, getExecutor("SlaveEventLoop-" + masterHost + ":" + masterPort));
            this.slaveContext.setSlaveClient(slaveClient);
            slaveClient.start();
        }

        public Context getContext() {
            return slaveContext;
        }
    }

    private class ConsoleNode extends Thread {
        private String host;
        private int port;
        private int timeout;
        private List<InetSocketAddress> forwarderNodeAddresses;
        private List<InetSocketAddress> slaveNodeAddresses;

        public ConsoleNode(String host, int port, int timeout, List<InetSocketAddress> forwarderNodeAddresses, List<InetSocketAddress> slaveNodeAddresses) {
            this.host = host;
            this.port = port;
            this.timeout = timeout;
            this.forwarderNodeAddresses = forwarderNodeAddresses;
            this.slaveNodeAddresses = slaveNodeAddresses;
        }

        @Override
        public void run() {
            ConsoleContext consoleContext = new ConsoleContext(this.host, this.port, this.timeout, forwarderIsCluster, masterIsCluster);
            forwarderNodeAddresses.forEach(address -> {
                String host = address.getHostName();
                int port = address.getPort();
                ForwarderNode forwarderNode = new ForwarderNode("Console", cacheManager, host, port, forwarderIsCluster, true);
                consoleContext.setForwarderContext((ForwarderContext) forwarderNode.getContext());
                forwarderNode.start();
            });
            slaveNodeAddresses.forEach(address -> {
                String host = address.getHostName();
                int port = address.getPort();
                SlaveNode slaveNode = new SlaveNode("Console", cacheManager, host, port, true, masterIsCluster);
                consoleContext.setSlaveContext((SlaveContext) slaveNode.getContext());
                slaveNode.start();
            });
            ConsoleServer consoleServer = new ConsoleServer(consoleContext, getExecutor("Console-Boss"), getExecutor("Console-Worker"));
            consoleServer.start();
        }
    }
}

package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CommandCache;
import com.dahuaboke.redisx.forwarder.ForwarderClient;
import com.dahuaboke.redisx.forwarder.ForwarderContext;
import com.dahuaboke.redisx.slave.SlaveClient;
import com.dahuaboke.redisx.slave.SlaveContext;
import com.dahuaboke.redisx.web.WebContext;
import com.dahuaboke.redisx.web.WebServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    private CommandCache commandCache;
    private boolean forwarderIsCluster;

    public Context() {
        this(false);
    }

    public Context(boolean forwarderIsCluster) {
        commandCache = new CommandCache(forwarderIsCluster);
        this.forwarderIsCluster = forwarderIsCluster;
    }

    public void start(List<InetSocketAddress> forwardNodeAddresses, List<InetSocketAddress> slaveNodeAddresses, InetSocketAddress webAddress) {
        forwardNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new ForwarderNode(commandCache, host, port, forwarderIsCluster).start();
        });
        slaveNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new SlaveNode(commandCache, host, port).start();
        });
        String webHost = webAddress.getHostName();
        int webPort = webAddress.getPort();
        new WebNode(commandCache, webHost, webPort).start();
    }

    public boolean isAdapt(boolean forwarderIsCluster, Object obj) {
        return false;
    }

    private class ForwarderNode extends Thread {
        private CommandCache commandCache;
        private String forwardHost;
        private int forwardPort;
        private boolean forwarderIsCluster;

        public ForwarderNode(CommandCache commandCache, String forwardHost, int forwardPort, boolean forwarderIsCluster) {
            this.setName(Constant.PROJECT_NAME + "-ForwardNode-" + forwardHost + "-" + forwardPort);
            this.commandCache = commandCache;
            this.forwardHost = forwardHost;
            this.forwardPort = forwardPort;
            this.forwarderIsCluster = forwarderIsCluster;
        }

        @Override
        public void run() {
            ForwarderContext forwarderContext = new ForwarderContext(commandCache, forwardHost, forwardPort, forwarderIsCluster);
            commandCache.register(forwarderContext);
            ForwarderClient forwarderClient = new ForwarderClient(forwarderContext);
            forwarderClient.start();
        }
    }

    private class SlaveNode extends Thread {
        private CommandCache commandCache;
        private String masterHost;
        private int masterPort;

        public SlaveNode(CommandCache commandCache, String masterHost, int masterPort) {
            this.setName(Constant.PROJECT_NAME + "-SlaveNode-" + masterHost + "-" + masterPort);
            this.setDaemon(true);
            this.commandCache = commandCache;
            this.masterHost = masterHost;
            this.masterPort = masterPort;
        }

        @Override
        public void run() {
            SlaveContext slaveContext = new SlaveContext(commandCache, masterHost, masterPort);
            SlaveClient slaveClient = new SlaveClient(slaveContext);
            slaveClient.start();
        }
    }

    private class WebNode extends Thread {
        private CommandCache commandCache;
        private String host;
        private int port;

        public WebNode(CommandCache commandCache, int port) {
            this.commandCache = commandCache;
            this.port = port;
        }

        public WebNode(CommandCache commandCache, String host, int port) {
            this.commandCache = commandCache;
            this.host = host;
            this.port = port;
        }

        @Override
        public void run() {
            WebContext webContext = new WebContext(commandCache, host, port);
            commandCache.register(webContext);
            WebServer webServer = new WebServer(webContext);
            webServer.start();
        }
    }
}

package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CacheManager;
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
    private CacheManager cacheManager;
    private boolean forwarderIsCluster;

    public Context() {
        this(false);
    }

    public Context(boolean forwarderIsCluster) {
        cacheManager = new CacheManager(forwarderIsCluster);
        this.forwarderIsCluster = forwarderIsCluster;
    }

    public void start(List<InetSocketAddress> forwardNodeAddresses, List<InetSocketAddress> slaveNodeAddresses, InetSocketAddress webAddress, int webTimeout) {
        forwardNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new ForwarderNode(cacheManager, host, port, forwarderIsCluster).start();
        });
        slaveNodeAddresses.forEach(address -> {
            String host = address.getHostName();
            int port = address.getPort();
            new SlaveNode(cacheManager, host, port).start();
        });
        String webHost = webAddress.getHostName();
        int webPort = webAddress.getPort();
        new WebNode(cacheManager, webHost, webPort, webTimeout).start();
    }

    public boolean isAdapt(boolean forwarderIsCluster, Object obj) {
        return false;
    }

    private class ForwarderNode extends Thread {
        private CacheManager cacheManager;
        private String forwardHost;
        private int forwardPort;
        private boolean forwarderIsCluster;

        public ForwarderNode(CacheManager cacheManager, String forwardHost, int forwardPort, boolean forwarderIsCluster) {
            this.setName(Constant.PROJECT_NAME + "-ForwardNode-" + forwardHost + "-" + forwardPort);
            this.cacheManager = cacheManager;
            this.forwardHost = forwardHost;
            this.forwardPort = forwardPort;
            this.forwarderIsCluster = forwarderIsCluster;
        }

        @Override
        public void run() {
            ForwarderContext forwarderContext = new ForwarderContext(cacheManager, forwardHost, forwardPort, forwarderIsCluster);
            cacheManager.register(forwarderContext);
            ForwarderClient forwarderClient = new ForwarderClient(forwarderContext);
            forwarderClient.start();
        }
    }

    private class SlaveNode extends Thread {
        private CacheManager cacheManager;
        private String masterHost;
        private int masterPort;

        public SlaveNode(CacheManager cacheManager, String masterHost, int masterPort) {
            this.setName(Constant.PROJECT_NAME + "-SlaveNode-" + masterHost + "-" + masterPort);
            this.setDaemon(true);
            this.cacheManager = cacheManager;
            this.masterHost = masterHost;
            this.masterPort = masterPort;
        }

        @Override
        public void run() {
            SlaveContext slaveContext = new SlaveContext(cacheManager, masterHost, masterPort);
            SlaveClient slaveClient = new SlaveClient(slaveContext);
            slaveClient.start();
        }
    }

    private class WebNode extends Thread {
        private CacheManager cacheManager;
        private String host;
        private int port;
        private int timeout;

        public WebNode(CacheManager cacheManager, int port, int timeout) {
            this.cacheManager = cacheManager;
            this.port = port;
            this.timeout = timeout;
        }

        public WebNode(CacheManager cacheManager, String host, int port, int timeout) {
            this.cacheManager = cacheManager;
            this.host = host;
            this.port = port;
            this.timeout = timeout;
        }

        @Override
        public void run() {
            WebContext webContext = new WebContext(cacheManager, host, port, timeout);
            cacheManager.register(webContext);
            WebServer webServer = new WebServer(webContext);
            webServer.start();
        }
    }
}

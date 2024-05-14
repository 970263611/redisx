package com.dahuaboke.redisx.slave;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * 2024/5/6 16:18
 * auth: dahua
 * desc: 从节点上下文
 */
public class SlaveContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(SlaveContext.class);

    private CacheManager cacheManager;
    private String masterHost;
    private int masterPort;
    private Channel slaveChannel;
    private String localHost;
    private int localPort;

    public SlaveContext(CacheManager cacheManager, String masterHost, int masterPort) {
        this.cacheManager = cacheManager;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public boolean publish(String command) {
        return cacheManager.publish(command);
    }

    public void setSlaveChannel(Channel slaveChannel) {
        this.slaveChannel = slaveChannel;
        InetSocketAddress inetSocketAddress = (InetSocketAddress) this.slaveChannel.localAddress();
        this.localHost = inetSocketAddress.getHostName();
        this.localPort = inetSocketAddress.getPort();
    }

    public String getLocalHost() {
        return localHost;
    }

    public int getLocalPort() {
        return localPort;
    }
}

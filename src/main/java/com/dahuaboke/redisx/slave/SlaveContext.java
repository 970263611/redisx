package com.dahuaboke.redisx.slave;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.console.handler.SlotInfoHandler;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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
    private int slotBegin;
    private int slotEnd;
    private SlaveClient slaveClient;
    private boolean isConsole;
    private boolean masterIsCluster;

    public SlaveContext(CacheManager cacheManager, String masterHost, int masterPort, boolean isConsole, boolean masterIsCluster) {
        this.cacheManager = cacheManager;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
        this.masterIsCluster = masterIsCluster;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public boolean publish(String msg) {
        if (!isConsole) {
            return cacheManager.publish(msg);
        } else {
            if (replyQueue == null) {
                throw new IllegalStateException("By console mode replyQueue need init");
            } else {
                return replyQueue.offer(msg);
            }
        }
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

    public void setSlaveClient(SlaveClient slaveClient) {
        this.slaveClient = slaveClient;
    }

    @Override
    public boolean isAdapt(boolean isMasterCluster, String command) {
        if (isMasterCluster && command != null) {
            int hash = calculateHash(command);
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ForwarderContext类型的context
            return true;
        }
    }

    public boolean isConsole() {
        return isConsole;
    }

    public void setSlotBegin(int slotBegin) {
        this.slotBegin = slotBegin;
    }

    public void setSlotEnd(int slotEnd) {
        this.slotEnd = slotEnd;
    }

    public boolean isMasterIsCluster() {
        return masterIsCluster;
    }

    @Override
    public String sendCommand(String command, int timeout) {
        if (replyQueue == null) {
            throw new IllegalStateException("By console mode replyQueue need init");
        } else {
            replyQueue.clear();
            slaveClient.sendCommand(command);
            try {
                return replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        }
    }

    public void close() {
        this.slaveClient.destroy();
    }
}

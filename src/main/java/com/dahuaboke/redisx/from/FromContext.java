package com.dahuaboke.redisx.from;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/6 16:18
 * auth: dahua
 * desc: 从节点上下文
 */
public class FromContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(FromContext.class);
    private String id;
    private CacheManager cacheManager;
    private String host;
    private int port;
    private Channel fromChannel;
    private String localHost;
    private int localPort;
    private int slotBegin;
    private int slotEnd;
    private FromClient fromClient;
    private boolean isConsole;
    private boolean fromIsCluster;

    public FromContext(CacheManager cacheManager, String host, int port, boolean isConsole, boolean fromIsCluster) {
        this.id = UUID.randomUUID().toString();
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
        this.fromIsCluster = fromIsCluster;
    }


    public String getId() {
        return id;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
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

    public void setFromChannel(Channel fromChannel) {
        this.fromChannel = fromChannel;
        InetSocketAddress inetSocketAddress = (InetSocketAddress) this.fromChannel.localAddress();
        this.localHost = inetSocketAddress.getHostName();
        this.localPort = inetSocketAddress.getPort();
    }

    public String getLocalHost() {
        return localHost;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setFromClient(FromClient fromClient) {
        this.fromClient = fromClient;
    }

    @Override
    public boolean isAdapt(boolean isMasterCluster, String command) {
        if (isMasterCluster && command != null) {
            int hash = calculateHash(command) % Constant.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
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

    public boolean isFromIsCluster() {
        return fromIsCluster;
    }

    @Override
    public String sendCommand(String command, int timeout) {
        if (replyQueue == null) {
            throw new IllegalStateException("By console mode replyQueue need init");
        } else {
            replyQueue.clear();
            fromClient.sendCommand(command);
            try {
                return replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        }
    }

    public void close() {
        this.fromClient.destroy();
        cacheManager.remove(this);
    }
}

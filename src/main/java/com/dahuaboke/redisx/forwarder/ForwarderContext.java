package com.dahuaboke.redisx.forwarder;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/13 10:38
 * auth: dahua
 * desc:
 */
public class ForwarderContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ForwarderContext.class);

    private CacheManager cacheManager;
    private String forwardHost;
    private int forwardPort;
    private boolean forwarderIsCluster;
    private int slotBegin;
    private int slotEnd;
    private ForwarderClient forwarderClient;

    public ForwarderContext(CacheManager cacheManager, String forwardHost, int forwardPort, boolean forwarderIsCluster, boolean isConsole) {
        this.cacheManager = cacheManager;
        this.forwardHost = forwardHost;
        this.forwardPort = forwardPort;
        this.forwarderIsCluster = forwarderIsCluster;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
    }

    public String getForwardHost() {
        return forwardHost;
    }

    public int getForwardPort() {
        return forwardPort;
    }

    public String listen() {
        CacheManager.CommandReference listen = cacheManager.listen(this);
        if (listen != null) {
            return listen.getContent();
        }
        return null;
    }

    public boolean callBack(String reply) {
        if (isConsole) {
            if (replyQueue == null) {
                throw new IllegalStateException("By console mode replyQueue need init");
            } else {
                //针对控制台返回特殊处理
                if (reply == null) {
                    reply = "(null)";
                }
                return replyQueue.offer(reply);
            }
        } else {
            return true;
        }
    }

    @Override
    public boolean isAdapt(boolean forwarderIsCluster, String command) {
        if (forwarderIsCluster && command != null) {
            int hash = calculateHash(command);
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ForwarderContext类型的context
            return true;
        }
    }

    @Override
    public String sendCommand(String command, int timeout) {
        if (replyQueue == null) {
            throw new IllegalStateException("By console mode replyQueue need init");
        } else {
            replyQueue.clear();
            forwarderClient.sendCommand(command);
            try {
                return replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        }
    }

    public void setSlotBegin(int slotBegin) {
        this.slotBegin = slotBegin;
    }

    public void setSlotEnd(int slotEnd) {
        this.slotEnd = slotEnd;
    }

    public void setForwarderClient(ForwarderClient forwarderClient) {
        this.forwarderClient = forwarderClient;
    }

    public boolean isForwarderIsCluster() {
        return forwarderIsCluster;
    }

    public void close() {
        this.forwarderClient.destroy();
    }
}

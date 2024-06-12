package com.dahuaboke.redisx.to;

import com.dahuaboke.redisx.Constant;
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
public class ToContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ToContext.class);

    private CacheManager cacheManager;
    private String host;
    private int port;
    private boolean toIsCluster;
    private int slotBegin;
    private int slotEnd;
    private ToClient toClient;

    public ToContext(CacheManager cacheManager, String host, int port, boolean toIsCluster, boolean isConsole) {
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.toIsCluster = toIsCluster;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
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
    public boolean isAdapt(boolean toIsCluster, String command) {
        if (toIsCluster && command != null) {
            int hash = calculateHash(command) % Constant.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
            return true;
        }
    }

    @Override
    public String sendCommand(String command, int timeout) {
        if (replyQueue == null) {
            throw new IllegalStateException("By console mode replyQueue need init");
        } else {
            replyQueue.clear();
            toClient.sendCommand(command);
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

    public void setClient(ToClient toClient) {
        this.toClient = toClient;
    }

    public boolean isToIsCluster() {
        return toIsCluster;
    }

    public void close() {
        this.toClient.destroy();
    }
}
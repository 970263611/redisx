package com.dahuaboke.redisx.forwarder;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

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
    private List<CacheManager.CommandReference> callBackList;

    public ForwarderContext(CacheManager cacheManager, String forwardHost, int forwardPort, boolean forwarderIsCluster) {
        this.cacheManager = cacheManager;
        this.forwardHost = forwardHost;
        this.forwardPort = forwardPort;
        this.forwarderIsCluster = forwarderIsCluster;
        this.callBackList = new LinkedList();
    }

    public String getForwardHost() {
        return forwardHost;
    }

    public int getForwardPort() {
        return forwardPort;
    }

    public String listen() {
        CacheManager.CommandReference listen = cacheManager.listen(this);
        if (listen.getCountDownLatch() == null) {
            callBackList.add(null);
        } else {
            callBackList.add(listen);
        }
        return listen.getContent();
    }

    public void callBack(String reply) {
        CacheManager.CommandReference commandReference = callBackList.remove(0);
        if (commandReference != null) {
            commandReference.setResult(reply);
            commandReference.getCountDownLatch().countDown();
        }
    }

    @Override
    public boolean isAdapt(boolean forwarderIsCluster, Object obj) {
        if (forwarderIsCluster && obj != null && obj instanceof String) {
            String command = (String) obj;
            int hash = calculateHash(command);
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ForwarderContext类型的context
            return true;
        }
    }

    private int calculateHash(String command) {
        String[] ary = command.split(" ");
        if (ary.length > 1) {
            return CRC16.crc16(ary[1].getBytes(StandardCharsets.UTF_8));
        } else {
            logger.warn("Command split length should > 1");
            return 0;
        }
    }
}

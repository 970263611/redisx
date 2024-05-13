package com.dahuaboke.redisx.cache;

import com.dahuaboke.redisx.CRC16;
import com.dahuaboke.redisx.forwarder.ForwarderContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 2024/5/13 10:45
 * auth: dahua
 * desc:
 */
public final class CommandCache {

    private static final Logger logger = LoggerFactory.getLogger(CommandCache.class);

    private Map<ForwarderContext, BlockingQueue<String>> forwarderCache = new HashMap();
    private boolean forwarderIsCluster;

    public CommandCache(boolean forwarderIsCluster) {
        this.forwarderIsCluster = forwarderIsCluster;
    }

    public void registerForwarder(ForwarderContext forwarderContext) {
        BlockingQueue<String> cache = new LinkedBlockingQueue();
        forwarderCache.put(forwarderContext, cache);
    }

    public boolean publish(String command) {
        for (Map.Entry<ForwarderContext, BlockingQueue<String>> entry : forwarderCache.entrySet()) {
            ForwarderContext k = entry.getKey();
            BlockingQueue<String> v = entry.getValue();
            if (forwarderIsCluster) {
                int hash = calculateHash(command);
                if (k.isAdapt(hash)) {
                    return v.offer(command);
                }
            } else {
                //非cluster，cache里面只有一组就是请求的这个node
                return v.offer(command);
            }
        }
        return false;
    }

    public String listen(ForwarderContext forwarderContext) {
        try {
            if (forwarderCache.containsKey(forwarderContext)) {
                //非cluster，cache里面只有一组就是请求的这个node
                return forwarderCache.get(forwarderContext).take();
            }
        } catch (InterruptedException e) {
            logger.error("Listener command thread interrupted");
        }
        return null;
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

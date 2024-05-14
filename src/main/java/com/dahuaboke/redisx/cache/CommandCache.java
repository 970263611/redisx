package com.dahuaboke.redisx.cache;

import com.dahuaboke.redisx.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Map<Context, BlockingQueue<String>> cache = new HashMap();
    private boolean forwarderIsCluster;

    public CommandCache(boolean forwarderIsCluster) {
        this.forwarderIsCluster = forwarderIsCluster;
    }

    public void register(Context context) {
        BlockingQueue<String> queue = new LinkedBlockingQueue();
        cache.put(context, queue);
    }

    /**
     * 非web模式走这个方法，因为只有web才能context发布方和监听发布方是一致的
     *
     * @param command
     * @return
     */
    public boolean publish(String command) {
        for (Map.Entry<Context, BlockingQueue<String>> entry : cache.entrySet()) {
            Context k = entry.getKey();
            BlockingQueue<String> v = entry.getValue();
            if (k.isAdapt(forwarderIsCluster, command)) {
                return v.offer(command);
            }
        }
        return false;
    }

    /**
     * web模式发布方式
     *
     * @param context
     * @param command
     * @return
     */
    public boolean publish(Context context, String command) {
        BlockingQueue<String> queue = cache.get(context);
        if (queue != null) {
            return queue.offer(command);
        }
        return false;
    }

    public String listen(Context context) {
        try {
            if (cache.containsKey(context)) {
                return cache.get(context).take();
            }
        } catch (InterruptedException e) {
            logger.error("Listener command thread interrupted");
        }
        return null;
    }
}

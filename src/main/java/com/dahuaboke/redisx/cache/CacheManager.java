package com.dahuaboke.redisx.cache;

import com.dahuaboke.redisx.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/13 10:45
 * auth: dahua
 * desc:
 */
public final class CacheManager {

    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);
    private Map<Context, BlockingQueue<CommandReference>> cache = new HashMap();
    private boolean toIsCluster;
    private boolean fromIsCluster;

    public CacheManager(boolean toIsCluster, boolean fromIsCluster) {
        this.toIsCluster = toIsCluster;
        this.fromIsCluster = fromIsCluster;
    }

    public void register(Context context) {
        BlockingQueue<CommandReference> queue = new LinkedBlockingQueue();
        cache.put(context, queue);
    }

    /**
     * 非web模式走这个方法，因为只有web需要回调传递
     *
     * @param command
     * @return
     */
    public boolean publish(String command) {
        return publish(new CommandReference(command));
    }

    public boolean publish(CommandReference commandReference) {
        String command = commandReference.getContent();
        for (Map.Entry<Context, BlockingQueue<CommandReference>> entry : cache.entrySet()) {
            Context k = entry.getKey();
            BlockingQueue<CommandReference> v = entry.getValue();
            if (k.isAdapt(toIsCluster, command)) {
                return v.offer(commandReference);
            }
        }
        return false;
    }

    public CommandReference listen(Context context) {
        try {
            if (cache.containsKey(context)) {
                return cache.get(context).poll(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            logger.error("Listener command thread interrupted");
        }
        return null;
    }

    public static class CommandReference {
        private String content;

        public CommandReference(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }
    }
}

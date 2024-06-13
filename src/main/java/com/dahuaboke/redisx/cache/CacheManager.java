package com.dahuaboke.redisx.cache;

import com.dahuaboke.redisx.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 2024/5/13 10:45
 * auth: dahua
 * desc:
 */
public final class CacheManager {

    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);
    private List<Context> contexts = new ArrayList();
    private Map<Context, BlockingQueue<CommandReference>> cache = new HashMap();
    private boolean toIsCluster;
    private boolean fromIsCluster;
    private AtomicBoolean isMaster = new AtomicBoolean(false);
    private AtomicBoolean fromStarted = new AtomicBoolean(false);

    public CacheManager(boolean toIsCluster, boolean fromIsCluster) {
        this.toIsCluster = toIsCluster;
        this.fromIsCluster = fromIsCluster;
    }

    /**
     * 注册from to组的所有context
     *
     * @param context
     */
    public void register(Context context) {
        contexts.add(context);
    }

    /**
     * 获取from to组的所有context
     */
    public List<Context> getAllContexts() {
        return contexts;
    }

    /**
     * 注册to的context，用于接收消息
     *
     * @param context
     */
    public void registerTo(Context context) {
        BlockingQueue<CommandReference> queue = new LinkedBlockingQueue();
        cache.put(context, queue);
    }

    public void remove(Context context) {
        cache.remove(context);
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

    public boolean isMaster() {
        return isMaster.get();
    }

    public void setIsMaster(boolean isMaster) {
        this.isMaster.set(isMaster);
    }

    public boolean fromIsStarted() {
        return fromStarted.get();
    }

    public void setFromIsStarted(boolean isMaster) {
        this.fromStarted.set(isMaster);
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

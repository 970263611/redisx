package com.dahuaboke.redisx.cache;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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
    private boolean fromIsCluster;
    private String fromPassword;
    private boolean toIsCluster;
    private String toPassword;
    private AtomicBoolean isMaster = new AtomicBoolean(false);
    private AtomicBoolean fromStarted = new AtomicBoolean(false);
    private String id = UUID.randomUUID().toString().replaceAll("-", "");
    private Map<String, NodeMessage> nodeMessages = new ConcurrentHashMap();
    private String redisVersion;

    public CacheManager(String redisVersion, boolean fromIsCluster, String fromPassword, boolean toIsCluster, String toPassword) {
        this.redisVersion = redisVersion;
        this.fromIsCluster = fromIsCluster;
        this.fromPassword = fromPassword;
        this.toIsCluster = toIsCluster;
        this.toPassword = toPassword;
    }

    /**
     * 服务唯一id
     */
    public String getId() {
        return id;
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

    public boolean checkHasNeedWriteCommand(Context context) {
        return cache.get(context).size() > 0;
    }

    /**
     * 非web模式走这个方法，因为只有web需要回调传递
     *
     * @param command
     * @param length
     * @param fromContext
     * @return
     */
    public boolean publish(String command, Integer length, FromContext fromContext) {
        return publish(new CommandReference(command, length, fromContext));
    }

    public boolean publish(CommandReference commandReference) {
        String command = commandReference.getContent();
        for (Map.Entry<Context, BlockingQueue<CommandReference>> entry : cache.entrySet()) {
            Context k = entry.getKey();
            BlockingQueue<CommandReference> v = entry.getValue();
            if (k.isAdapt(toIsCluster, command)) {
                boolean offer = v.offer(commandReference);
                if (!offer) {
                    logger.error("Publish command error, queue size [{}]", v.size());
                }
                return offer;
            }
        }
        logger.error("Key hash not adapt any toContext [{}]", command);
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

    public NodeMessage getNodeMessage(String host, int port) {
        return nodeMessages.get(host + ":" + port);
    }

    public void setNodeMessage(String host, int port, String masterId, long offset) {
        nodeMessages.put(host + ":" + port, new NodeMessage(host, port, masterId, offset));
    }

    public Map<String, NodeMessage> getAllNodeMessages() {
        return nodeMessages;
    }

    public void clearAllNodeMessages() {
        nodeMessages.clear();
    }

    public String getFromPassword() {
        return fromPassword;
    }

    public String getToPassword() {
        return toPassword;
    }

    public String getRedisVersion() {
        return redisVersion;
    }

    public static class NodeMessage {
        private String host;
        private int port;
        private String masterId;
        private long offset;

        public NodeMessage(String host, int port, String masterId, long offset) {
            this.host = host;
            this.port = port;
            this.masterId = masterId;
            this.offset = offset;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getMasterId() {
            return masterId;
        }

        public long getOffset() {
            return offset;
        }
    }

    public static class CommandReference {
        private String content;
        private Integer length;
        private FromContext fromContext;

        public CommandReference(String content, Integer length, FromContext fromContext) {
            this.content = content;
            this.length = length;
            this.fromContext = fromContext;
        }

        public String getContent() {
            return content;
        }

        public Integer getLength() {
            return length;
        }

        public FromContext getFromContext() {
            return fromContext;
        }
    }
}

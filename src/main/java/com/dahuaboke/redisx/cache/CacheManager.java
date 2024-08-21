package com.dahuaboke.redisx.cache;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.command.from.SyncCommand;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.dahuaboke.redisx.enums.Mode;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.ClusterInfoHandler;
import com.dahuaboke.redisx.to.ToContext;
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
    private Map<Context, BlockingQueue<SyncCommand>> cache = new HashMap();
    private Mode fromMode;
    private String fromPassword;
    private Mode toMode;
    private String toPassword;
    private AtomicBoolean isMaster = new AtomicBoolean(false);
    private AtomicBoolean fromStarted = new AtomicBoolean(false);

    private AtomicBoolean toStarted = new AtomicBoolean(false);
    private String id = UUID.randomUUID().toString().replaceAll("-", "");
    private Map<String, NodeMessage> nodeMessages = new ConcurrentHashMap();
    private String redisVersion;
    private Set<ClusterInfoHandler.SlotInfo> fromClusterNodesInfo = new HashSet<>();
    private Set<ClusterInfoHandler.SlotInfo> toClusterNodesInfo = new HashSet<>();
    private ConsoleContext consoleContext;
    private Map<String, Boolean> flushDb = new HashMap();

    public CacheManager(String redisVersion, Mode fromMode, String fromPassword, Mode toMode, String toPassword) {
        this.redisVersion = redisVersion;
        this.fromMode = fromMode;
        this.fromPassword = fromPassword;
        this.toMode = toMode;
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
        BlockingQueue<SyncCommand> queue = new LinkedBlockingQueue();
        cache.put(context, queue);
    }

    public void remove(Context context) {
        Iterator<Context> iterator = contexts.iterator();
        while (iterator.hasNext()) {
            Context next = iterator.next();
            if (next == context) {
                iterator.remove();
            }
        }
        if (context instanceof ToContext) {
            cache.remove(context);
        }
    }

    public boolean checkHasNeedWriteCommand(Context context) {
        return cache.get(context).size() > 0;
    }

    public boolean publish(SyncCommand command) {
        String key = command.getKey();
        for (Map.Entry<Context, BlockingQueue<SyncCommand>> entry : cache.entrySet()) {
            Context k = entry.getKey();
            BlockingQueue<SyncCommand> v = entry.getValue();
            if (k.isAdapt(toMode, key)) {
                boolean offer = v.offer(command);
                int size = v.size();
                if (size > 10000) {
                    logger.warn("Cache has command size [{}]", size);
                }
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

    public boolean toIsStarted() {
        return toStarted.get();
    }

    public void setToStarted(boolean toStarted) {
        this.toStarted.set(toStarted);
    }

    public boolean fromIsStarted() {
        return fromStarted.get();
    }

    public void setFromIsStarted(boolean isMaster) {
        this.fromStarted.set(isMaster);
    }

    public SyncCommand listen(Context context) {
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

    public void closeAllFrom() {
        List<Context> allContexts = getAllContexts();
        for (Context cont : allContexts) {
            if (cont instanceof FromContext) {
                ((FromContext) cont).close();
            }
        }
    }

    public void closeAllTo() {
        List<Context> allContexts = getAllContexts();
        for (Context cont : allContexts) {
            if (cont instanceof ToContext) {
                ((ToContext) cont).close();
            }
        }
    }

    public Set<ClusterInfoHandler.SlotInfo> getFromClusterNodesInfo() {
        return fromClusterNodesInfo;
    }

    public void addFromClusterNodesInfo(ClusterInfoHandler.SlotInfo fromClusterNodesInfo) {
        this.fromClusterNodesInfo.add(fromClusterNodesInfo);
    }

    public ClusterInfoHandler.SlotInfo getFromClusterNodeInfoByIpAndPort(String ip, int port) {
        for (ClusterInfoHandler.SlotInfo slotInfo : fromClusterNodesInfo) {
            if (ip.equals(slotInfo.getIp()) && port == slotInfo.getPort()) {
                return slotInfo;
            }
        }
        return null;
    }

    public ClusterInfoHandler.SlotInfo getToClusterNodeInfoByIpAndPort(String ip, int port) {
        for (ClusterInfoHandler.SlotInfo slotInfo : toClusterNodesInfo) {
            if (ip.equals(slotInfo.getIp()) && port == slotInfo.getPort()) {
                return slotInfo;
            }
        }
        return null;
    }

    public Set<ClusterInfoHandler.SlotInfo> getToClusterNodesInfo() {
        return toClusterNodesInfo;
    }

    public void addToClusterNodesInfo(ClusterInfoHandler.SlotInfo toClusterNodesInfo) {
        this.toClusterNodesInfo.add(toClusterNodesInfo);
    }

    public void clearFromNodesInfo() {
        this.fromClusterNodesInfo.clear();
    }

    public void clearToNodesInfo() {
        this.toClusterNodesInfo.clear();
    }

    public ConsoleContext getConsoleContext() {
        return consoleContext;
    }

    public void setConsoleContext(ConsoleContext consoleContext) {
        this.consoleContext = consoleContext;
    }

    public boolean isFlushDb(String host, int port) {
        Boolean b = flushDb.get(host + port);
        return b == null ? false : b;
    }

    public void setFlushDb(String host, int port, boolean flushDb) {
        this.flushDb.put(host + port, flushDb);
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
}

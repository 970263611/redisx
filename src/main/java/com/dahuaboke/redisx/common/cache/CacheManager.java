package com.dahuaboke.redisx.common.cache;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.LimitedList;
import com.dahuaboke.redisx.common.command.from.SyncCommand;
import com.dahuaboke.redisx.common.enums.FlushState;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.enums.ShutdownState;
import com.dahuaboke.redisx.console.ConsoleContext;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.ClusterInfoHandler;
import com.dahuaboke.redisx.handler.SentinelInfoHandler;
import com.dahuaboke.redisx.to.ToContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

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
    private Set<SentinelInfoHandler.SlaveInfo> fromSentinelNodesInfo = new HashSet<>();
    private Set<SentinelInfoHandler.SlaveInfo> toSentinelNodesInfo = new HashSet<>();
    private ConsoleContext consoleContext;
    private FlushState flushState = FlushState.END;
    private InetSocketAddress fromSentinelMaster;
    private InetSocketAddress toSentinelMaster;
    private Map<String, LimitedList<Long>> fromWriteCount = new HashMap<>();
    private Map<String, LimitedList<Long>> toWriteCount = new HashMap<>();
    private Map<String, Long> errorCount = new HashMap<>();
    private boolean alwaysFullSync;
    private ShutdownState shutdownState;
    private List<Pattern> patterns;

    public CacheManager(String redisVersion, Mode fromMode, String fromPassword, Mode toMode, String toPassword, boolean alwaysFullSync, List<String> filterRules) {
        this.redisVersion = redisVersion;
        this.fromMode = fromMode;
        this.fromPassword = fromPassword;
        this.toMode = toMode;
        this.toPassword = toPassword;
        this.alwaysFullSync = alwaysFullSync;
        if(filterRules != null) {
            patterns = new ArrayList<>();
            filterRules.forEach(f -> {
                patterns.add(Pattern.compile(f));
            });
        }
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
        contexts.remove(context);
        if (context instanceof ToContext) {
            cache.remove(context);
        } else if (context instanceof ConsoleContext) {
            consoleContext = null;
        }
    }

    public boolean checkHasNeedWriteCommand(Context context) {
        return getOverstockSize(context) > 0;
    }

    public int getOverstockSize(Context context) {
        return cache.get(context).size();
    }

    public boolean publish(SyncCommand command) {
        byte[] key = command.getKey();
        for (Map.Entry<Context, BlockingQueue<SyncCommand>> entry : cache.entrySet()) {
            Context k = entry.getKey();
            BlockingQueue<SyncCommand> v = entry.getValue();
            if (k.isAdapt(toMode, key)) {
                boolean offer = v.offer(command);
                int size = v.size();
                if (size > 10000 && size / 1000 * 1000 == size) {
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
        Iterator<Context> iterator = allContexts.iterator();
        while (iterator.hasNext()) {
            Context context = iterator.next();
            if (context instanceof FromContext) {
                FromContext fromContext = (FromContext) context;
                iterator.remove();
                fromContext.close();
            }
        }
        setFromIsStarted(false);
    }

    public void closeAllTo() {
        List<Context> allContexts = getAllContexts();
        Iterator<Context> iterator = allContexts.iterator();
        while (iterator.hasNext()) {
            Context context = iterator.next();
            if (context instanceof ToContext) {
                ToContext toContext = (ToContext) context;
                iterator.remove();
                toContext.close();
            }
        }
    }

    public Set<ClusterInfoHandler.SlotInfo> getFromClusterNodesInfo() {
        return fromClusterNodesInfo;
    }

    public void addFromClusterNodesInfo(ClusterInfoHandler.SlotInfo fromClusterNodeInfo) {
        this.fromClusterNodesInfo.add(fromClusterNodeInfo);
    }

    public ClusterInfoHandler.SlotInfo getFromClusterNodeInfoByIpAndPort(String ip, int port) {
        for (ClusterInfoHandler.SlotInfo slotInfo : fromClusterNodesInfo) {
            if (ip.equals(slotInfo.getIp()) && port == slotInfo.getPort()) {
                if (slotInfo.isActiveMaster()) {
                    return slotInfo;
                } else {
                    return getFromClusterMasterNodeInfoById(slotInfo.getMasterId());
                }
            }
        }
        return null;
    }

    public ClusterInfoHandler.SlotInfo getFromClusterMasterNodeInfoById(String id) {
        for (ClusterInfoHandler.SlotInfo slotInfo : fromClusterNodesInfo) {
            if (slotInfo.getId().endsWith(id)) {
                if (slotInfo.isActiveMaster()) {
                    return slotInfo;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    public Set<SentinelInfoHandler.SlaveInfo> getFromSentinelNodesInfo() {
        return fromSentinelNodesInfo;
    }

    public void addFromSentinelNodesInfo(SentinelInfoHandler.SlaveInfo fromSentinelNodeInfo) {
        this.fromSentinelNodesInfo.add(fromSentinelNodeInfo);
    }

    public Set<SentinelInfoHandler.SlaveInfo> getToSentinelNodesInfo() {
        return toSentinelNodesInfo;
    }

    public void addToSentinelNodesInfo(SentinelInfoHandler.SlaveInfo toSentinelNodeInfo) {
        this.toSentinelNodesInfo.add(toSentinelNodeInfo);
    }

    public SentinelInfoHandler.SlaveInfo getFromSentinelNodeInfoByIpAndPort(String ip, int port) {
        for (SentinelInfoHandler.SlaveInfo slaveInfo : fromSentinelNodesInfo) {
            if (ip.equals(slaveInfo.getIp()) && port == slaveInfo.getPort()) {
                return slaveInfo;
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

    public FlushState getFlushState() {
        return flushState;
    }

    public void setFlushState(FlushState flushState) {
        this.flushState = flushState;
    }

    public InetSocketAddress getFromSentinelMaster() {
        return fromSentinelMaster;
    }

    public void setFromSentinelMaster(InetSocketAddress fromSentinelMaster) {
        this.fromSentinelMaster = fromSentinelMaster;
    }

    public InetSocketAddress getToSentinelMaster() {
        return toSentinelMaster;
    }

    public void setToSentinelMaster(InetSocketAddress toSentinelMaster) {
        this.toSentinelMaster = toSentinelMaster;
    }

    public Map<String, LimitedList<Long>> getFromWriteCount() {
        return fromWriteCount;
    }

    public LimitedList<Long> getFromWriteCount(String host, int port) {
        return fromWriteCount.get(host + ":" + port);
    }

    public void addFromWriteCount(String host, int port, Long fromCount) {
        String key = host + ":" + port;
        if (this.fromWriteCount.containsKey(key)) {
            fromWriteCount.get(key).add(fromCount);
        } else {
            fromWriteCount.put(key, new LimitedList<>(60));
        }
    }

    public Map<String, LimitedList<Long>> getToWriteCount() {
        return toWriteCount;
    }

    public LimitedList<Long> getToWriteCount(String host, int port) {
        return toWriteCount.get(host + ":" + port);
    }

    public void addToWriteCount(String host, int port, Long toCount) {
        String key = host + ":" + port;
        if (this.toWriteCount.containsKey(key)) {
            toWriteCount.get(key).add(toCount);
        } else {
            toWriteCount.put(key, new LimitedList<>(60));
        }
    }

    public Long getErrorCount(String host, int port) {
        return errorCount.get(host + port);
    }

    public void setErrorCount(String host, int port, Long err) {
        this.errorCount.put(host + port, err);
    }

    public Mode getFromMode() {
        return fromMode;
    }

    public Mode getToMode() {
        return toMode;
    }

    public boolean isAlwaysFullSync() {
        return alwaysFullSync;
    }

    public ShutdownState getShutdownState() {
        return shutdownState;
    }

    public void setShutdownState(ShutdownState shutdownState) {
        this.shutdownState = shutdownState;
    }

    public List<Pattern> getPatterns() {
        return patterns;
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

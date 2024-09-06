package com.dahuaboke.redisx.common.cache;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.Redisx;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.ClusterInfoHandler;
import com.dahuaboke.redisx.handler.SentinelInfoHandler;
import com.dahuaboke.redisx.to.ToContext;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * author: dahua
 * date: 2024/9/2 10:40
 */
public class CacheMonitor {

    private CacheManager cacheManager;
    private List<Map<String, Object>> fromSentinelNodesMap = new ArrayList<>();
    private List<Map<String, Object>> toSentinelNodesMap = new ArrayList<>();
    private List<Map<String, Object>> fromClusterNodesMap = new ArrayList<>();
    private List<Map<String, Object>> toClusterNodesMap = new ArrayList<>();
    private List<Map<String, Object>> fromRedisxNodesMap = new ArrayList<>();
    private List<Map<String, Object>> toRedisxNodesMap = new ArrayList<>();
    private Map<String, Object> headConfig = new LinkedHashMap<>();
    private Map<String, Object> fromConfig = new LinkedHashMap<>();
    private Map<String, Object> toConfig = new LinkedHashMap<>();

    public CacheMonitor(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    public void setConfig(Redisx.Config config) {
        buildConfig(config);
    }

    public Map buildMonitor() {
        clear();
        addSentinelMessage();
        addClusterMessage();
        addRedisxNodesMessage();
        return new LinkedHashMap() {{
            put("headConfig", headConfig);
            put("fromConfig", fromConfig);
            put("toConfig", toConfig);
            put("fromSentinelNodes", fromSentinelNodesMap);
            put("toSentinelNodes", toSentinelNodesMap);
            put("fromClusterNodes", fromClusterNodesMap);
            put("toClusterNodes", toClusterNodesMap);
            put("fromRedisxNodes", fromRedisxNodesMap);
            put("toRedisxNodes", toRedisxNodesMap);
        }};
    }

    public void clear() {
        fromClusterNodesMap.clear();
        toClusterNodesMap.clear();
        fromSentinelNodesMap.clear();
        toSentinelNodesMap.clear();
        fromRedisxNodesMap.clear();
        toRedisxNodesMap.clear();
    }

    public void addSentinelMessage() {
        addFromSentinelMessage();
        addToSentinelMessage();
    }

    public void addFromSentinelMessage() {
        if (Mode.SENTINEL == cacheManager.getFromMode()) {
            InetSocketAddress fromSentinelMaster = cacheManager.getFromSentinelMaster();
            fromClusterNodesMap.add(new HashMap<String, Object>() {{
                put("host", fromSentinelMaster.getHostString());
                put("port", fromSentinelMaster.getPort());
                put("type", "master");
                put("active", "true");
            }});
            Set<SentinelInfoHandler.SlaveInfo> fromSentinelNodesInfo = cacheManager.getFromSentinelNodesInfo();
            fromSentinelNodesInfo.forEach(f -> {
                fromClusterNodesMap.add(new HashMap<String, Object>() {{
                    put("host", f.getIp());
                    put("port", f.getPort());
                    put("type", "slave");
                    put("active", f.isActive());
                }});
            });
        }
    }

    public void addToSentinelMessage() {
        if (Mode.SENTINEL == cacheManager.getToMode()) {
            InetSocketAddress toSentinelMaster = cacheManager.getToSentinelMaster();
            toClusterNodesMap.add(new HashMap<String, Object>() {{
                put("host", toSentinelMaster.getHostString());
                put("port", toSentinelMaster.getPort());
                put("type", "master");
                put("active", "true");
            }});
            Set<SentinelInfoHandler.SlaveInfo> toSentinelNodesInfo = cacheManager.getToSentinelNodesInfo();
            toSentinelNodesInfo.forEach(t -> {
                toClusterNodesMap.add(new HashMap<String, Object>() {{
                    put("host", t.getIp());
                    put("port", t.getPort());
                    put("type", "slave");
                    put("active", t.isActive());
                }});
            });
        }
    }

    public void addClusterMessage() {
        addFromClusterMessage();
        addToClusterMessage();
    }

    private void addFromClusterMessage() {
        if (Mode.CLUSTER == cacheManager.getFromMode()) {
            Set<ClusterInfoHandler.SlotInfo> fromClusterNodesInfo = cacheManager.getFromClusterNodesInfo();
            fromClusterNodesInfo.forEach(f -> {
                fromClusterNodesMap.add(new HashMap<String, Object>() {{
                    put("host", f.getIp());
                    put("port", f.getPort());
                    put("masterId", f.getMasterId());
                    put("type", f.isMaster() ? "master" : "slave");
                    put("active", f.isActive());
                }});
            });
        }
    }

    private void addToClusterMessage() {
        if (Mode.CLUSTER == cacheManager.getToMode()) {
            Set<ClusterInfoHandler.SlotInfo> toClusterNodesInfo = cacheManager.getToClusterNodesInfo();
            toClusterNodesInfo.forEach(t -> {
                toClusterNodesMap.add(new HashMap<String, Object>() {{
                    put("host", t.getIp());
                    put("port", t.getPort());
                    put("masterId", t.getMasterId());
                    put("type", t.isMaster() ? "master" : "slave");
                    put("active", t.isActive());
                }});
            });
        }
    }

    private void addRedisxNodesMessage() {
        List<Context> allContexts = cacheManager.getAllContexts();
        allContexts.forEach(a -> {
            if (a instanceof FromContext) {
                FromContext fromContext = (FromContext) a;
                CacheManager.NodeMessage nodeMessage = fromContext.getNodeMessage();
                fromRedisxNodesMap.add(new HashMap<String, Object>() {{
                    put("host", fromContext.getHost());
                    put("port", fromContext.getPort());
                    put("offset", nodeMessage.getOffset());
                    put("count", fromContext.getWriteCount());
                    put("tps", fromContext.getWriteTps(true));
                    put("error", fromContext.getErrorCount() == null ? 0 : fromContext.getErrorCount());
                }});
            } else if (a instanceof ToContext) {
                ToContext toContext = (ToContext) a;
                toRedisxNodesMap.add(new HashMap<String, Object>() {{
                    put("host", toContext.getHost());
                    put("port", toContext.getPort());
                    put("slot", toContext.getSlotBegin() + "-" + toContext.getSlotEnd());
                    put("count", toContext.getWriteCount());
                    put("tps", toContext.getWriteTps(false));
                    put("overstock", cacheManager.getOverstockSize(toContext));
                    put("error", toContext.getErrorCount() == null ? 0 : toContext.getErrorCount());
                }});
            }
        });
    }

    private void buildConfig(Redisx.Config config) {
        buildHeadConfig(config);
        buildFromConfig(config);
        buildToConfig(config);
    }

    private void buildHeadConfig(Redisx.Config config) {
        String switchFlag = config.getSwitchFlag();
        headConfig.put("切换标志", switchFlag);
        String logLevelGlobal = config.getLogLevelGlobal();
        headConfig.put("日志级别", logLevelGlobal);
        String id = cacheManager.getId();
        headConfig.put("节点标识", id);
        boolean master = cacheManager.isMaster();
        headConfig.put("是否主节点", master);
        boolean fromIsStarted = cacheManager.fromIsStarted();
        headConfig.put("[From]是否启动", fromIsStarted);
        boolean toIsStarted = cacheManager.toIsStarted();
        headConfig.put("[To]是否启动", toIsStarted);
    }

    private void buildFromConfig(Redisx.Config config) {
        Mode fromMode = config.getFromMode();
        fromConfig.put("模式", fromMode);
        String fromMasterName = config.getFromMasterName();
        fromConfig.put("主名称", fromMasterName);
        List<InetSocketAddress> fromAddresses = config.getFromAddresses();
        List<String> addr = new ArrayList<>();
        fromAddresses.forEach(f -> {
            addr.add(f.getHostString() + ":" + f.getPort());
        });
        fromConfig.put("地址列表", addr);
        String fromPassword = config.getFromPassword();
        fromConfig.put("密码", fromPassword);
        String redisVersion = config.getRedisVersion();
        fromConfig.put("Redis版本", redisVersion);
        boolean verticalScaling = config.isVerticalScaling();
        fromConfig.put("垂直扩展", verticalScaling);
        boolean connectMaster = config.isConnectMaster();
        fromConfig.put("强制连接主节点", connectMaster);
        boolean alwaysFullSync = config.isAlwaysFullSync();
        fromConfig.put("是否每次全量同步", alwaysFullSync);
        boolean syncRdb = config.isSyncRdb();
        fromConfig.put("是否同步Rdb", syncRdb);
    }

    private void buildToConfig(Redisx.Config config) {
        Mode toMode = config.getToMode();
        toConfig.put("模式", toMode);
        String toMasterName = config.getToMasterName();
        toConfig.put("主名称", toMasterName);
        List<InetSocketAddress> toAddresses = config.getToAddresses();
        List<String> addr = new ArrayList<>();
        toAddresses.forEach(t -> {
            addr.add(t.getHostString() + ":" + t.getPort());
        });
        toConfig.put("地址列表", addr);
        String toPassword = config.getToPassword();
        toConfig.put("密码", toPassword);
        int toFlushSize = config.getToFlushSize();
        toConfig.put("写入阈值", toFlushSize);
        boolean immediate = config.isImmediate();
        toConfig.put("是否强一致模式", immediate);
        int immediateResendTimes = config.getImmediateResendTimes();
        toConfig.put("强一致模式失败重试次数", immediateResendTimes);
        boolean flushDb = config.isFlushDb();
        toConfig.put("是否清空Redis数据", flushDb);
    }
}

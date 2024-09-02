package com.dahuaboke.redisx.to;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.common.command.from.SyncCommand;
import com.dahuaboke.redisx.common.enums.FlushState;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.handler.ClusterInfoHandler;
import com.dahuaboke.redisx.handler.SentinelInfoHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/13 10:38
 * auth: dahua
 * desc:
 */
public class ToContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ToContext.class);
    private static final String lua1 = "local v = redis.call('GET',KEYS[1]);\n" + "    if v then\n" + "        return v;\n" + "    else\n" + "        local result = redis.call('SET',KEYS[1],ARGV[1]);\n" + "        return result;\n" + "    end";
    private static final String lua2 = "local v = redis.call('GET',KEYS[1]);\n" + "if string.match(v,ARGV[1]) then\n redis.call('SET',KEYS[1],ARGV[2]);\nend";
    private static final String luaFlush = "redis.call(flushall);\n" + lua1;
    private int slotBegin;
    private int slotEnd;
    private ToClient toClient;
    private boolean immediate;
    private int immediateResendTimes;
    private String switchFlag;
    private int flushSize;
    private CountDownLatch nodesInfoFlag;
    private boolean isNodesInfoContext;
    private boolean flushDb;
    private String toMasterName;

    public ToContext(CacheManager cacheManager, String host, int port, Mode fromMode, Mode toMode, boolean consoleStart, boolean immediate, int immediateResendTimes, String switchFlag, int flushSize, boolean isNodesInfoContext, boolean flushDb, String toMasterName) {
        super(cacheManager, host, port, fromMode, toMode, consoleStart);
        if (consoleStart) {
            replyQueue = new LinkedBlockingDeque();
        }
        this.immediate = immediate;
        this.immediateResendTimes = immediateResendTimes;
        this.switchFlag = switchFlag;
        this.flushSize = flushSize;
        this.isNodesInfoContext = isNodesInfoContext;
        this.toMasterName = toMasterName;
        this.flushDb = flushDb;
        if (isNodesInfoContext) {
            nodesInfoFlag = new CountDownLatch(1);
        } else if (Mode.CLUSTER == toMode) {
            ClusterInfoHandler.SlotInfo toClusterNodeInfo = cacheManager.getToClusterNodeInfoByIpAndPort(host, port);
            if (toClusterNodeInfo != null) {
                this.slotBegin = toClusterNodeInfo.getSlotStart();
                this.slotEnd = toClusterNodeInfo.getSlotEnd();
            } else {
                throw new IllegalStateException("Slot info error");
            }
        }
    }

    public String getId() {
        return cacheManager.getId();
    }

    public SyncCommand listen() {
        return cacheManager.listen(this);
    }

    public boolean callBack(String reply) {
        if (consoleStart) {
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
    public boolean isAdapt(Mode mode, byte[] command) {
        if (Mode.CLUSTER == mode && command != null) {
            int hash = calculateHash(command) % Constants.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
            return true;
        }
    }

    @Override
    public String sendCommand(Object command, int timeout) {
        return sendCommand(command, timeout, false, null, false);
    }

    public String sendCommand(Object command, int timeout, String key) {
        return sendCommand(command, timeout, false, key, false);
    }

    public String sendCommand(Object command, int timeout, boolean unCheck) {
        return sendCommand(command, timeout, unCheck, null, unCheck);
    }

    public String sendCommand(Object command, int timeout, boolean unCheck, boolean needIsSuccess) {
        return sendCommand(command, timeout, unCheck, null, needIsSuccess);
    }

    public String sendCommand(Object command, int timeout, boolean unCheck, String key, boolean needIsSuccess) {
        if (consoleStart) {
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
        } else {
            if (unCheck) {
                return String.valueOf(toClient.sendCommand(command, needIsSuccess));
            } else {
                List<Context> allContexts = cacheManager.getAllContexts();
                for (Context context : allContexts) {
                    if (context instanceof ToContext) {
                        ToContext toContext = (ToContext) context;
                        String flag;
                        if (key != null) {
                            flag = key;
                        } else {
                            flag = (String) command;
                        }
                        if (toContext.isAdapt(toMode, flag.getBytes())) {
                            return toContext.sendCommand(command, 1000, true, null, true);
                        }
                    }
                }
                logger.error("Command adapt error {}", command);
                return "false";
            }
        }
    }

    public void setClient(ToClient toClient) {
        this.toClient = toClient;
    }

    public void close() {
        cacheManager.remove(this);
        if (nodesInfoFlag != null) {
            nodesInfoFlag.countDown();
        }
        this.toClient.destroy();
    }

    public void isMaster(boolean isMaster) {
        cacheManager.setIsMaster(isMaster);
    }

    public boolean toStarted() {
        return cacheManager.toIsStarted();
    }

    public void setToStarted(boolean started) {
        cacheManager.setToStarted(started);
    }

    public Map<String, CacheManager.NodeMessage> getAllNodeMessages() {
        return cacheManager.getAllNodeMessages();
    }

    public void setNodeMessage(String host, int port, String masterId, long offset) {
        cacheManager.setNodeMessage(host, port, masterId, offset);
    }

    public void clearAllNodeMessages() {
        cacheManager.clearAllNodeMessages();
    }

    public void preemptMaster() {
        List<String> commands = new ArrayList() {{
            add("EVAL");
            add(lua1);
            add("1");
            add(switchFlag);
            add(preemptMasterCommand());
        }};
        this.sendCommand(commands, 1000, true);
    }

    public void preemptMasterAndFlushAll() {
        List<String> commands = new ArrayList() {{
            add("EVAL");
            add(luaFlush);
            add("1");
            add(switchFlag);
            add(preemptMasterCommand());
        }};
        this.sendCommand(commands, 1000, true, false);
    }

    public void preemptMasterCompulsory() {
        this.sendCommand(buildPreemptMasterCompulsoryCommand(), 1000, switchFlag);
    }

    public boolean preemptMasterCompulsoryWithCheckId() {
        List<String> commands = new ArrayList() {{
            add("EVAL");
            add(lua2);
            add("1");
            add(switchFlag);
            add(getId());
            add(preemptMasterCommand());
        }};
        return Boolean.valueOf(this.sendCommand(commands, 1000, switchFlag));
    }

    public String buildPreemptMasterCompulsoryCommand() {
        return "set " + switchFlag + " " + preemptMasterCommand();
    }

    private String preemptMasterCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constants.PROJECT_NAME);
        sb.append("|");
        sb.append(this.getId());
        sb.append("|");
        getAllNodeMessages().forEach((k, v) -> {
            String masterId = v.getMasterId();
            if (masterId != null) {
                sb.append(k);
                sb.append("&");
                sb.append(v.getMasterId());
                sb.append("&");
                sb.append(v.getOffset());
                sb.append(";");
            }
        });
        sb.append("|");
        sb.append(System.currentTimeMillis());
        return new String(sb);
    }

    public String getPassword() {
        return cacheManager.getToPassword();
    }

    public boolean isImmediate() {
        return immediate;
    }

    public int getImmediateResendTimes() {
        return immediateResendTimes;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public void addSlotInfo(ClusterInfoHandler.SlotInfo slotInfo) {
        this.cacheManager.addToClusterNodesInfo(slotInfo);
    }

    public boolean nodesInfoGetSuccess(int timeout) {
        try {
            return nodesInfoFlag.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public void setToNodesInfoGetSuccess() {
        if (nodesInfoFlag != null) {
            nodesInfoFlag.countDown();
        }
    }

    public boolean isNodesInfoContext() {
        return isNodesInfoContext;
    }

    public boolean isFlushDb() {
        return flushDb;
    }

    public String getToMasterName() {
        return toMasterName;
    }

    public void setSentinelMasterInfo(String host, int port) {
        cacheManager.setToSentinelMaster(new InetSocketAddress(host, port));
    }

    public void addSentinelSlaveInfo(List<SentinelInfoHandler.SlaveInfo> toSentinelNodesInfo) {
        cacheManager.addToSentinelNodesInfo(toSentinelNodesInfo);
    }

    public FlushState getFlushState() {
        return cacheManager.getFlushState();
    }

    public void setFlushState(FlushState flushState) {
        cacheManager.setFlushState(flushState);
    }

    public int getSlotBegin() {
        return slotBegin;
    }

    public int getSlotEnd() {
        return slotEnd;
    }

    public void putWriteCount() {
        cacheManager.addToWriteCount(host, port, writeCount);
    }

    public Long getWriteTps() {
        return cacheManager.getToWriteCount(host, port).getTps();
    }

    @Override
    public String toString() {
        return "ToContext{" + "host='" + host + '\'' + ", port=" + port + ", slotBegin=" + slotBegin + ", slotEnd=" + slotEnd + ", immediate=" + immediate + ", immediateResendTimes=" + immediateResendTimes + ", switchFlag='" + switchFlag + '\'' + ", flushSize=" + flushSize + ", isClose=" + isClose + ", consoleStart=" + consoleStart + ", toMode=" + toMode + ", fromMode=" + fromMode + '}';
    }
}

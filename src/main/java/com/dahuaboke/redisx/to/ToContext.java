package com.dahuaboke.redisx.to;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/13 10:38
 * auth: dahua
 * desc:
 */
public class ToContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ToContext.class);
    private static final String lua = "local v = redis.call('GET',KEYS[1]);\n" +
            "    if v then\n" +
            "        return v;\n" +
            "    else\n" +
            "        local result = redis.call('SET',KEYS[1],ARGV[1]);\n" +
            "        return result;\n" +
            "    end";
    private CacheManager cacheManager;
    private String host;
    private int port;
    private boolean toIsCluster;
    private int slotBegin;
    private int slotEnd;
    private ToClient toClient;

    public ToContext(CacheManager cacheManager, String host, int port, boolean toIsCluster, boolean isConsole) {
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.toIsCluster = toIsCluster;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
    }

    public String getId() {
        return cacheManager.getId();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public CacheManager.CommandReference listen() {
        return cacheManager.listen(this);
    }

    public boolean callBack(String reply) {
        if (isConsole) {
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
    public boolean isAdapt(boolean toIsCluster, String command) {
        if (toIsCluster && command != null) {
            int hash = calculateHash(command) % Constant.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
            return true;
        }
    }

    @Override
    public String sendCommand(Object command, int timeout) {
        if (isConsole) {
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
            toClient.sendCommand(command);
            return null;
        }
    }

    public void setSlotBegin(int slotBegin) {
        this.slotBegin = slotBegin;
    }

    public void setSlotEnd(int slotEnd) {
        this.slotEnd = slotEnd;
    }

    public void setClient(ToClient toClient) {
        this.toClient = toClient;
    }

    public boolean isToIsCluster() {
        return toIsCluster;
    }

    public void close() {
        this.toClient.destroy();
        cacheManager.remove(this);
    }

    public void isMaster(boolean isMaster) {
        cacheManager.setIsMaster(isMaster);
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
            add(lua);
            add("1");
            add(Constant.DR_KEY);
            add(preemptMasterCommand());
        }};
        this.sendCommand(commands, 1000);
    }

    public void preemptMasterCompulsory() {
        this.sendCommand("set " + Constant.DR_KEY + " " + preemptMasterCommand(), 1000);
    }

    private String preemptMasterCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constant.PROJECT_NAME);
        sb.append("|");
        sb.append(this.getId());
        sb.append("|");
        getAllNodeMessages().forEach((k, v) -> {
            sb.append(k);
            sb.append("&");
            sb.append(v.getMasterId());
            sb.append("&");
            sb.append(v.getOffset());
            sb.append(";");
        });
        sb.append("|");
        sb.append(System.currentTimeMillis());
        return new String(sb);
    }

    public String getPassword() {
        return cacheManager.getToPassword();
    }
}

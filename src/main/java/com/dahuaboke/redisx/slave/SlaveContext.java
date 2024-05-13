package com.dahuaboke.redisx.slave;

import com.dahuaboke.redisx.cache.CommandCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/6 16:18
 * auth: dahua
 * desc: 从节点上下文
 */
public class SlaveContext {

    private static final Logger logger = LoggerFactory.getLogger(SlaveContext.class);

    private CommandCache commandCache;
    private String masterHost;
    private int masterPort;

    public SlaveContext(CommandCache commandCache, String masterHost, int masterPort) {
        this.commandCache = commandCache;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public String getMasterHost() {
        return masterHost;
    }

    public int getMasterPort() {
        return masterPort;
    }

    public boolean send(String command) {
        return commandCache.send(command);
    }
}

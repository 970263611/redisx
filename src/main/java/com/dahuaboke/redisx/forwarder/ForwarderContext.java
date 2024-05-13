package com.dahuaboke.redisx.forwarder;

import com.dahuaboke.redisx.cache.CommandCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:38
 * auth: dahua
 * desc:
 */
public class ForwarderContext {

    private static final Logger logger = LoggerFactory.getLogger(ForwarderContext.class);

    private CommandCache commandCache;
    private String forwardHost;
    private int forwardPort;
    private boolean forwarderIsCluster;
    private int slotBegin;
    private int slotEnd;

    public ForwarderContext(CommandCache commandCache, String forwardHost, int forwardPort, boolean forwarderIsCluster) {
        this.commandCache = commandCache;
        this.forwardHost = forwardHost;
        this.forwardPort = forwardPort;
        this.forwarderIsCluster = forwarderIsCluster;
    }

    public String getForwardHost() {
        return forwardHost;
    }

    public int getForwardPort() {
        return forwardPort;
    }

    public String listen() {
        return commandCache.listen(this);
    }

    public boolean isAdapt(int hash) {
        return hash >= slotBegin && hash <= slotEnd;
    }
}

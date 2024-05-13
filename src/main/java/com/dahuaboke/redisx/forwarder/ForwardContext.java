package com.dahuaboke.redisx.forwarder;

import com.dahuaboke.redisx.cache.CommandCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:38
 * auth: dahua
 * desc:
 */
public class ForwardContext {

    private static final Logger logger = LoggerFactory.getLogger(ForwardContext.class);

    private CommandCache commandCache;
    private String forwardHost;
    private int forwardPort;

    public ForwardContext(CommandCache commandCache, String forwardHost, int forwardPort) {
        this.commandCache = commandCache;
        this.forwardHost = forwardHost;
        this.forwardPort = forwardPort;
    }

    public String getForwardHost() {
        return forwardHost;
    }

    public int getForwardPort() {
        return forwardPort;
    }

    public String listen() {
        return commandCache.listen();
    }
}

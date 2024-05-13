package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CommandCache;
import com.dahuaboke.redisx.forwarder.ForwardContext;
import com.dahuaboke.redisx.slave.SlaveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);

    private SlaveContext slaveContext;
    private ForwardContext forwardContext;

    public Context(String masterHost, int masterPort, String forwardHost, int forwardPort) {
        CommandCache commandCache = new CommandCache();
        slaveContext = new SlaveContext(commandCache, masterHost, masterPort);
        forwardContext = new ForwardContext(commandCache, forwardHost, forwardPort);
    }

    public SlaveContext getSlaveContext() {
        return slaveContext;
    }

    public ForwardContext getForwardContext() {
        return forwardContext;
    }
}

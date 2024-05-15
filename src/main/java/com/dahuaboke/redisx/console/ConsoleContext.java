package com.dahuaboke.redisx.console;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.forwarder.ForwarderContext;
import com.dahuaboke.redisx.slave.SlaveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 2024/5/15 10:27
 * auth: dahua
 * desc:
 */
public class ConsoleContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleContext.class);
    private String host;
    private int port;
    private int timeout;
    private boolean forwarderIsCluster;
    private boolean masterIsCluster;
    private List<ForwarderContext> forwarderContexts = new ArrayList();
    private List<SlaveContext> slaveContexts = new ArrayList();

    public ConsoleContext(String host, int port, int timeout, boolean forwarderIsCluster, boolean masterIsCluster) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.forwarderIsCluster = forwarderIsCluster;
        this.masterIsCluster = masterIsCluster;
    }

    public void setForwarderContext(ForwarderContext forwarderContext) {
        forwarderContexts.add(forwarderContext);
    }

    public void setSlaveContext(SlaveContext slaveContext) {
        slaveContexts.add(slaveContext);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String sendCommand(String command, String type) {
        if ("left".equalsIgnoreCase(type)) {
            int slaveSize = slaveContexts.size();
            if (slaveSize > 1) {
                logger.warn("Master size should 1,but {}", slaveSize);
            }
            for (SlaveContext slaveContext : slaveContexts) {
                if (slaveContext.isAdapt(masterIsCluster, command)) {
                    return slaveContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else if ("right".equalsIgnoreCase(type)) {
            int forwarderSize = forwarderContexts.size();
            if (forwarderSize > 1) {
                logger.warn("Forwarder size should 1,but {}", forwarderSize);
            }
            for (ForwarderContext forwarderContext : forwarderContexts) {
                if (forwarderContext.isAdapt(forwarderIsCluster, command)) {
                    return forwarderContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else {
            throw new IllegalArgumentException("Type is error, should be left or right");
        }
    }
}

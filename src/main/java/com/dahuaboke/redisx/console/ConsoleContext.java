package com.dahuaboke.redisx.console;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
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
    private boolean toIsCluster;
    private boolean fromIsCluster;
    private List<ToContext> toContexts = new ArrayList();
    private List<FromContext> fromContexts = new ArrayList();

    public ConsoleContext(String host, int port, int timeout, boolean toIsCluster, boolean fromIsCluster) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.toIsCluster = toIsCluster;
        this.fromIsCluster = fromIsCluster;
    }

    public void setToContext(ToContext toContext) {
        toContexts.add(toContext);
    }

    public void setFromContext(FromContext fromContext) {
        fromContexts.add(fromContext);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String sendCommand(String command, String type) {
        if ("left".equalsIgnoreCase(type)) {
            int fromSize = fromContexts.size();
            if (fromSize > 1) {
                logger.warn("Master size should 1,but {}", fromSize);
            }
            for (FromContext fromContext : fromContexts) {
                if (fromContext.isAdapt(fromIsCluster, command)) {
                    return fromContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else if ("right".equalsIgnoreCase(type)) {
            int toSize = toContexts.size();
            if (toSize > 1) {
                logger.warn("To size should 1,but {}", toSize);
            }
            for (ToContext toContext : toContexts) {
                if (toContext.isAdapt(toIsCluster, command)) {
                    return toContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else {
            throw new IllegalArgumentException("Type is error, should be left or right");
        }
    }
}

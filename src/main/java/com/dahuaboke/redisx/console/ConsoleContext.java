package com.dahuaboke.redisx.console;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
        super(fromIsCluster, toIsCluster);
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
        List<String> list = Arrays.asList(command.split(" "));
        if (list.size() < 2 || list.get(0).equalsIgnoreCase("keys")) {
            return "Unsupported command: " + list;
        }
        if ("from".equalsIgnoreCase(type)) {
            for (FromContext fromContext : fromContexts) {
                if (fromContext.isAdapt(fromIsCluster, list.get(1))) {
                    return fromContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else if ("to".equalsIgnoreCase(type)) {
            for (ToContext toContext : toContexts) {
                if (toContext.isAdapt(toIsCluster, list.get(1))) {
                    return toContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else {
            throw new IllegalArgumentException("Type is error, should be [from] or [to]");
        }
    }
}

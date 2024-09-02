package com.dahuaboke.redisx.console;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.common.cache.CacheMonitor;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 2024/5/15 10:27
 * auth: dahua
 * desc:
 */
public class ConsoleContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleContext.class);
    private int timeout;
    private List<ToContext> toContexts = new ArrayList();
    private List<FromContext> fromContexts = new ArrayList();
    private ConsoleServer consoleServer;
    private CacheMonitor cacheMonitor;

    public ConsoleContext(CacheManager cacheManager, CacheMonitor cacheMonitor, String host, int port, int timeout, Mode toMode, Mode fromMode) {
        super(cacheManager, host, port, fromMode, toMode, true);
        this.cacheMonitor = cacheMonitor;
        this.timeout = timeout;
    }

    public void setToContext(ToContext toContext) {
        toContexts.add(toContext);
    }

    public void setFromContext(FromContext fromContext) {
        fromContexts.add(fromContext);
    }

    public String sendCommand(String command, String type) {
        List<String> list = Arrays.asList(command.split(" "));
        if (list.size() < 2 || list.get(0).equalsIgnoreCase("keys")) {
            return "Unsupported command: " + list;
        }
        if ("from".equalsIgnoreCase(type)) {
            for (FromContext fromContext : fromContexts) {
                if (fromContext.isAdapt(fromMode, list.get(1))) {
                    return fromContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else if ("to".equalsIgnoreCase(type)) {
            for (ToContext toContext : toContexts) {
                if (toContext.isAdapt(toMode, list.get(1))) {
                    return toContext.sendCommand(command, timeout);
                }
            }
            return null;
        } else {
            throw new IllegalArgumentException("Type is error, should be [from] or [to]");
        }
    }

    public void close() {
        consoleServer.destroy();
        Iterator<FromContext> iterator = fromContexts.iterator();
        while (iterator.hasNext()) {
            FromContext fromContext = iterator.next();
            iterator.remove();
            fromContext.close();
        }
        Iterator<ToContext> iterator1 = toContexts.iterator();
        while (iterator1.hasNext()) {
            ToContext toContext = iterator1.next();
            iterator1.remove();
            toContext.close();
        }
    }

    public void setConsoleServer(ConsoleServer consoleServer) {
        this.consoleServer = consoleServer;
    }

    public CacheMonitor getCacheMonitor() {
        return cacheMonitor;
    }
}

package com.dahuaboke.redisx.web;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CommandCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/14 10:40
 * auth: dahua
 * desc:
 */
public class WebContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(WebContext.class);
    private CommandCache commandCache;
    private String host;
    private int port;

    public WebContext(CommandCache commandCache, String host, int port) {
        this.commandCache = commandCache;
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean publish(Context context, String command) {
        return commandCache.publish(context, command);
    }
}

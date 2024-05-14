package com.dahuaboke.redisx.web;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/14 10:40
 * auth: dahua
 * desc:
 */
public class WebContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(WebContext.class);
    private CacheManager cacheManager;
    private String host;
    private int port;
    private long timeout;

    public WebContext(CacheManager cacheManager, String host, int port, int timeout) {
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.timeout = timeout;
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

    public CacheManager.CommandReference publish(String command) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        CacheManager.CommandReference commandReference = new CacheManager.CommandReference(command, countDownLatch);
        if (cacheManager.publish(commandReference)) {
            return commandReference;
        }
        return null;
    }

    public void listen(CacheManager.CommandReference commandReference) {
        try {
            CountDownLatch countDownLatch = commandReference.getCountDownLatch();
            countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            commandReference.setResult(null);
            return;
        }
    }
}

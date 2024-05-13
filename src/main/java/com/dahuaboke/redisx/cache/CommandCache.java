package com.dahuaboke.redisx.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 2024/5/13 10:45
 * auth: dahua
 * desc:
 */
public class CommandCache {

    private static final Logger logger = LoggerFactory.getLogger(CommandCache.class);

    private BlockingQueue<String> cache;

    public CommandCache() {
        cache = new LinkedBlockingQueue();
    }

    public boolean send(String command) {
        return cache.offer(command);
    }

    public String listen() {
        try {
            return cache.take();
        } catch (InterruptedException e) {
            logger.error("Listener command thread interrupted");
            return null;
        }
    }
}

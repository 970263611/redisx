package com.dahuaboke.redisx.sync;

import com.dahuaboke.redisx.core.Context;
import com.dahuaboke.redisx.netty.RedisClient;

/**
 * author: dahua
 * date: 2024/3/1 9:40
 */
public class SyncClient {

    private String masterHost;
    private int masterPort;
    private RedisClient redisClient;
    private Context context;
    private SyncReceiver syncReceiver;

    public SyncClient(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public void start() {
        context = new Context();
        syncReceiver = new SyncReceiver(context);
        RedisClient redisClient = new RedisClient(masterHost, masterPort);
        redisClient.start(context);
        context.register(syncReceiver);
        context.send("replicaof " + masterHost + " " + masterPort);
    }

    public void destroy() {
        redisClient.destroy();
        context.destroy();
    }
}

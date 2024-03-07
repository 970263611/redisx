package com.dahuaboke.redisx.sync;

import com.dahuaboke.redisx.core.Context;
import com.dahuaboke.redisx.core.Receiver;
import com.dahuaboke.redisx.core.SlaveState;
import com.dahuaboke.redisx.netty.RedisClient;
import com.dahuaboke.redisx.netty.handler.RedisRdbHandler;
import io.netty.channel.ChannelPipeline;

import static com.dahuaboke.redisx.core.SlaveState.*;

/**
 * author: dahua
 * date: 2024/3/1 9:45
 */
public class SyncReceiver implements Receiver {

    private static final String OK = "OK";
    private static final String PONG = "PONG";
    private Context context;
    private RedisClient redisClient;
    private String masterId;
    private String offset;
    private volatile SlaveState slaveState = REPL_STATE_CONNECT;

    public SyncReceiver(Context context, RedisClient redisClient) {
        this.context = context;
        this.redisClient = redisClient;
    }

    public void connectMaster() {
        slaveState = REPL_STATE_CONNECTING;
        context.send("PING");
        slaveState = REPL_STATE_RECEIVE_PONG;
    }

    @Override
    public void receive(String callBack) {
//        if (result.size() == 1 && "PING".equals(result.get(0).toString())) {
//            context.send("PONG");
        //REPLCONF ACK<reploff>
//        }
        System.out.println(callBack);
        switch (slaveState) {
            case REPL_STATE_RECEIVE_PONG -> {
                if (PONG.equals(callBack)) {
                    slaveState = REPL_STATE_SEND_PORT;
                    context.send("REPLCONF listening-port 8080");
                    slaveState = REPL_STATE_RECEIVE_PORT;
                }
            }
            case REPL_STATE_RECEIVE_PORT -> {
                if (OK.equals(callBack)) {
                    slaveState = REPL_STATE_SEND_IP;
                    context.send("REPLCONF ip-address 127.0.0.1");
                    slaveState = REPL_STATE_RECEIVE_IP;
                }
            }
            case REPL_STATE_RECEIVE_IP -> {
                if (OK.equals(callBack)) {
                    slaveState = REPL_STATE_SEND_CAPA;
                    context.send("REPLCONF capa eof");
                    slaveState = REPL_STATE_RECEIVE_CAPA;
                }
            }
            case REPL_STATE_RECEIVE_CAPA -> {
                if (OK.equals(callBack)) {
                    slaveState = REPL_STATE_SEND_PSYNC;
                    context.send("PSYNC ? -1");
                    slaveState = REPL_STATE_RECEIVE_PSYNC;
                }
            }
            case REPL_STATE_RECEIVE_PSYNC -> {
                // TODO 接收文件
                if (callBack.startsWith("FULLRESYNC")) {
                    String[] s = callBack.split(" ");
                    ChannelPipeline pipeline = redisClient.getChannel().pipeline();
                    masterId = s[1];
                    offset = s[2];
                    pipeline.addFirst(new RedisRdbHandler(context));
                    slaveState = REPL_STATE_TRANSFER;
                }
            }
        }
    }
}

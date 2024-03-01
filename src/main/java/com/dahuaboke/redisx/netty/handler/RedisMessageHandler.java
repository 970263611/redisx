package com.dahuaboke.redisx.netty.handler;

import com.dahuaboke.redisx.core.Context;
import com.dahuaboke.redisx.core.Sender;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.redis.*;
import io.netty.util.CharsetUtil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * author: dahua
 * date: 2024/2/27 15:44
 */
public class RedisMessageHandler extends ChannelDuplexHandler implements Sender {

    private Context context;
    private Channel channel;

    public RedisMessageHandler(Context context) {
        this.context = context;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        context.register(this);
        this.channel = ctx.channel();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        List<String> result = new LinkedList();
        parseAggregatedRedisResponse(msg, result);
        context.callBack(parseResult(result));
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        String[] commands = ((String) msg).split("\\s+");
        List<RedisMessage> children = new ArrayList(commands.length);
        for (String cmdString : commands) {
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), cmdString)));
        }
        RedisMessage request = new ArrayRedisMessage(children);
        ctx.write(request, promise);
    }

    private void parseAggregatedRedisResponse(Object msg, List<String> result) {
        if (msg instanceof SimpleStringRedisMessage) {
            result.add("(string)  " + ((SimpleStringRedisMessage) msg).content());
        } else if (msg instanceof IntegerRedisMessage) {
            result.add("(integer) " + ((IntegerRedisMessage) msg).value());
        } else if (msg instanceof FullBulkStringRedisMessage) {
            result.add("(string)  " + getString((FullBulkStringRedisMessage) msg));
        } else if (msg instanceof ArrayRedisMessage) {
            for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
                parseAggregatedRedisResponse(child, result);
            }
        } else if (msg instanceof ErrorRedisMessage) {
            result.add("(error)   " + ((ErrorRedisMessage) msg).content());
        } else {
            result.add("(unknown) " + msg == null ? "null" : "");
        }
    }

    private String getString(FullBulkStringRedisMessage msg) {
        if (msg.isNull()) {
            return "(null)";
        }
        return msg.content().toString(CharsetUtil.UTF_8);
    }

    private String parseResult(List<String> result) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int a = 0; a < result.size(); a++) {
            if (a != 0) {
                stringBuilder.append("\r\n");
            }
            stringBuilder.append((a + 1) + ")");
            stringBuilder.append(" ");
            stringBuilder.append(result.get(a));
        }
        return new String(stringBuilder);
    }

    @Override
    public void send(String command) {
        if (channel.isActive()) {
            channel.writeAndFlush(command);
        } else {
            throw new RuntimeException();
        }
    }
}

package com.dahuaboke.redisx.handler;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 2024/5/13 14:41
 * auth: dahua
 * desc:
 */
public class CommandEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(CommandEncoder.class);

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        List<RedisMessage> children = new ArrayList();
        if (msg instanceof String) {
            String command = ((String) msg).replaceAll("^\\s+", "");//去除字符串左侧的所有空格
            char[] chars = command.toCharArray();
            StringBuilder sb = new StringBuilder();
            for (char aChar : chars) {
                if (aChar == ' ') {
                    children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), sb.toString())));
                    sb = new StringBuilder();
                } else {
                    sb.append(aChar);
                }
            }
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), sb.toString())));
        } else if (msg instanceof List) {
            List<String> commands = (List<String>) msg;
            for (String command : commands) {
                children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), command)));
            }
        }
        RedisMessage request = new ArrayRedisMessage(children);
        ctx.write(request, promise);
    }
}

package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.command.Command;
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
        String commond = ((String) msg).replaceAll("^\\s+", "");//去除字符串左侧的所有空格
        List<RedisMessage> children = new ArrayList();
        if(commond.startsWith(Constant.PROJECT_NAME)){
            //children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), commond.substring(Constant.PROJECT_NAME.length()))));
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), "EVAL")));
            String lua = "local v = redis.call('GET',KEYS[1]);\n" +
                    "    if v then\n" +
                    "        return v;\n" +
                    "    else\n" +
                    "        local result = redis.call('SET',KEYS[1],ARGV[1]);\n" +
                    "        return result;\n" +
                    "    end";
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), lua)));
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), 1+"")));
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), Constant.DR_KEY)));
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), "abcde")));
        }else {
            char[] chars = commond.toCharArray();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < chars.length; i++) {
                if (chars[i] == ' ') {
                    children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), sb.toString())));
                    sb = new StringBuilder();
                } else {
                    sb.append(chars[i]);
                }
            }
            children.add(new FullBulkStringRedisMessage(ByteBufUtil.writeUtf8(ctx.alloc(), sb.toString())));
        }
        RedisMessage request = new ArrayRedisMessage(children);
        ctx.write(request, promise);
    }
}

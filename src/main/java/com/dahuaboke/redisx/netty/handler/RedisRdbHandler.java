package com.dahuaboke.redisx.netty.handler;

import com.dahuaboke.redisx.core.Context;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.LinkedList;

/**
 * author: dahua
 * date: 2024/3/7 9:53
 */
public class RedisRdbHandler extends ChannelInboundHandlerAdapter {

    private Context context;
    private int rdbLength = 0;
    private int rdbVersion = 0;

    public RedisRdbHandler(Context context) {
        this.context = context;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
//            ByteBuf in = (ByteBuf) msg;
//            if (rdbLength == 0) {
//                LinkedList<Byte> list = new LinkedList();
//                byte b;
//                while (true) {
//                    while ((b = in.readByte()) != '\r') {
//                        list.add(b);
//                    }
//                    if ((b = in.readByte()) == '\n') {
//                        break;
//                    } else {
//                        list.add(b);
//                    }
//                }
//                byte[] bytes = new byte[list.size()];
//                for (int i = 0; i < list.size(); i++) {
//                    bytes[i] = list.get(i);
//                }
//                rdbLength = Integer.parseInt(new String(bytes).replaceFirst("\\$", ""));
//                System.out.println("length:" + rdbLength);
//            } else {
//                byte[] bytes = new byte[5];
//                for (int a = 0; a < bytes.length; a++) {
//                    bytes[a] = in.readByte();
//                }
//                String prefix = new String(bytes);
//                if ("REDIS".equals(prefix)) {
//                    ctx.channel().pipeline().remove(this);
//                    bytes = new byte[4];
//                    for (int a = 0; a < bytes.length; a++) {
//                        bytes[a] = in.readByte();
//                    }
//                    rdbVersion = Integer.parseInt(new String(bytes));
//                    System.out.println("version:" + rdbVersion);
//                    while (true) {
//                        int type = in.readByte() & 0xff;
//                    }
//                } else {
//                    rdbLength = 0;
//                    context.send("PSYNC ? -1");
//                }
//            }
        }
    }
}

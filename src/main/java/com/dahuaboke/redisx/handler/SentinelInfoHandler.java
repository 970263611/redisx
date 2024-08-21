package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 2024/8/20 18:00
 * auth: cdl
 * desc: 通过哨兵获取当前主节点ip和端口
 */
public class SentinelInfoHandler extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(CommandEncoder.class);
    private Context context;
    private String masterName;

    public SentinelInfoHandler(Context context, String masterName) {
        super(context);
        this.context = context;
        this.masterName = masterName;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        sendMasterCommand(ctx);
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead2(ChannelHandlerContext ctx, String msg) throws Exception {
        parseMasterMessage(ctx, msg);
    }

    private void sendMasterCommand(ChannelHandlerContext ctx) {
        logger.info("Beginning sentinel get master command");
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            //不需要去pipeline的底部，所以直接ctx.write
            ctx.writeAndFlush(Constant.SENTINEL_GET_MASTER + masterName);
        }
    }

    private void parseMasterMessage(ChannelHandlerContext ctx, String msg) {
        logger.info("Beginning sentinel master message parse");
        if (msg != null) {
            String[] arr = msg.split(" ");
            String masterIp = arr[0];
            int masterPort = Integer.parseInt(arr[1]);
            if (context instanceof FromContext) {
                FromContext fromContext = (FromContext) context;
                fromContext.setSentinelMasterInfo(masterIp, masterPort);
                ctx.pipeline().remove(this);
                fromContext.setFromNodesInfoGetSuccess();
            } else if (context instanceof ToContext) {
                ToContext toContext = (ToContext) context;
                toContext.setSentinelMasterInfo(masterIp, masterPort);
                ctx.pipeline().remove(this);
                toContext.setToNodesInfoGetSuccess();
            }
        }
    }
}

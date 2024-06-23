package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:37
 * auth: dahua
 * desc:
 */
public class SyncCommandListener extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommandListener.class);
    private ToContext toContext;

    public SyncCommandListener(Context toContext) {
        this.toContext = (ToContext) toContext;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Thread thread = new Thread(() -> {
            while (!toContext.isClose()) {
                if (ctx.channel().pipeline().get(Constant.SLOT_HANDLER_NAME) == null) {
                    CacheManager.CommandReference reference = toContext.listen();
                    if (reference != null) {
                        FromContext fromContext = reference.getFromContext();
                        long offset = fromContext.getOffset();
                        boolean changeDb = false;
                        int fromDb = fromContext.getDb();
                        if (toContext.getDb(fromContext) != fromDb) {
                            String changeDbCommand = Constant.SELECT_PREFIX + fromDb;
                            ctx.writeAndFlush(changeDbCommand);
                            toContext.setDb(fromContext, fromDb);
                            changeDb = true;
                            logger.debug("Write change db command success [{}]", changeDbCommand);
                        }
                        String command = reference.getContent();
                        ctx.writeAndFlush(command);
                        Integer length = reference.getLength();
                        if (length != null) {
                            int pingSize = reference.getPingSize();
                            if (pingSize > 0) {
                                //ping指令固定14字节*1\r\n$4\r\nping\r\n
                                offset += pingSize * 14L;
                                logger.debug("Update offset because receive ping command size [{}], new offset [{}]", pingSize, offset);
                            }
                            if (changeDb) {
                                if (fromDb > 9) {
                                    //*2\r\n$6\r\nselect\r\n$2\r\nxx\r\n
                                    offset += 24;
                                } else {
                                    //*2\r\n$6\r\nselect\r\n$1\r\nx\r\n
                                    offset += 23;
                                }
                                logger.debug("Update offset because db change db index [{}], new offset [{}]", offset, fromDb);
                            }
                            long newOffset = offset + length;
                            fromContext.setOffset(newOffset);
                            logger.debug("Write command success [{}] length [{}], before offset [{}] new offset [{}]", command, length, offset, newOffset);
                        } else {
                            //rdb的切库指令不需要添加offset，避免rdb切库后正常切库指令无法添加offset（库号一致），这里设置成-2
                            toContext.setDb(fromContext, -2);
                            logger.debug("Write command success [{}], offset are not calculated, offset [{}]", command, offset);
                        }
                    }
                }
            }
        });
        thread.setName(Constant.PROJECT_NAME + "-To-Writer-" + toContext.getHost() + ":" + toContext.getPort());
        thread.start();
    }

    @Override
    public void channelRead2(ChannelHandlerContext ctx, String reply) throws Exception {
        logger.debug("Receive redis reply [{}]", reply);
        toContext.callBack(reply);
    }
}

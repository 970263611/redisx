package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.LimitedList;
import com.dahuaboke.redisx.common.enums.FlushState;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/6/12 17:10
 * auth: dahua
 * desc: 主备策略处理器
 */
public class DRHandler extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(DRHandler.class);
    private ToContext toContext;
    //10次标志不变从升为主
    private final LimitedList<String> limitedList = new LimitedList(10) {{
        add("INIT");
    }};

    public DRHandler(Context toContext) {
        super(toContext);
        this.toContext = (ToContext) toContext;
    }

    @Override
    public void channelRead2(ChannelHandlerContext ctx, String reply) throws Exception {
        if (reply.startsWith(Constants.PROJECT_NAME)) {
            String[] split = reply.split("\\|");
            if (split.length != 4) {
                toContext.preemptMasterCompulsory();
            } else {
                if (!Constants.PROJECT_NAME.equals(split[0])) {
                    toContext.preemptMasterCompulsory();
                } else {
                    if (toContext.getId().equals(split[1])) { //主节点是自己
                        //校验清空to数据
                        if (FlushState.PREPARE == toContext.getFlushState()) {
                            toContext.setFlushState(FlushState.BEGINNING);
                        } else {
                            toContext.preemptMasterCompulsory();
                        }
                        toContext.isMaster(true);
                    } else { //主节点非自己
                        toContext.isMaster(false);
                        toContext.setFlushState(FlushState.END);
                        toContext.clearAllNodeMessages();
                        String nodeMessagesStr = split[2];
                        if (!"".equals(nodeMessagesStr)) {
                            String[] nodeMessagesSplit = nodeMessagesStr.split(";");
                            for (String nodeMessageStr : nodeMessagesSplit) {
                                String[] messageSplit = nodeMessageStr.split("&", -1);
                                String masterId = messageSplit[1];
                                if (masterId != null && !"null".equalsIgnoreCase(masterId)) {
                                    String[] hostAndPort = messageSplit[0].split(":");
                                    String host = hostAndPort[0];
                                    int port = Integer.parseInt(hostAndPort[1]);
                                    long offset = Long.parseLong(messageSplit[2]);
                                    logger.trace("Sync host [{}] port [{}] masterId [{}] offset [{}]", host, port, masterId, offset);
                                    toContext.setNodeMessage(host, port, masterId, offset);
                                }
                            }
                        }
                        //这里不用时间区间判断是因为无法保证各服务器时间相同
                        String random = split[3];
                        limitedList.add(random);
                        if (limitedList.checkNeedUpgradeMaster()) {
                            toContext.preemptMasterCompulsory();
                        }
                    }
                }
            }
        } else {
            if (reply.startsWith(Constants.ERROR_REPLY_PREFIX)) {
                logger.error("Receive redis error reply [{}]", reply);
                if (toContext.isConsoleStart()) {
                    toContext.addErrorCount();
                }
            }
            if (toContext.isConsoleStart()) {
                toContext.callBack(reply);
            }
        }
    }
}

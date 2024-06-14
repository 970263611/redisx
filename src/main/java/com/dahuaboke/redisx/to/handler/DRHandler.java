package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * 2024/6/12 17:10
 * auth: dahua
 * desc: 主备策略处理器
 */
public class DRHandler extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(DRHandler.class);
    private ToContext toContext;
    //10次标志不变从升为主
    private final LimitedList<String> limitedList = new LimitedList(10);

    public DRHandler(ToContext toContext) {
        this.toContext = toContext;
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception {
        if (reply.startsWith(Constant.PROJECT_NAME)) {
            String[] split = reply.split("\\|");
            if (split.length != 4) {
                toContext.preemptMasterCompulsory();
            } else {
                if (!Constant.PROJECT_NAME.equals(split[0])) {
                    toContext.preemptMasterCompulsory();
                } else {
                    if (toContext.getId().equals(split[1])) { //主节点是自己
                        toContext.isMaster(true);
                        toContext.preemptMasterCompulsory();
                    } else { //主节点非自己
                        long offset = Long.parseLong(split[2]);
                        toContext.setOffset(offset);
                        //这里不用时间区间判断是因为无法保证各服务器时间相同
                        String random = split[3];
                        limitedList.add(random);
                        if (limitedList.checkNeedUpgradeMaster()) {
                            toContext.isMaster(true);
                        } else {
                            toContext.isMaster(false);
                        }
                    }
                }
            }
        }
    }

    private class LimitedList<L> extends LinkedList<L> {
        private final int limitSize;

        public LimitedList(int limitSize) {
            this.limitSize = limitSize;
        }

        @Override
        public boolean add(L l) {
            super.add(l);
            while (size() > limitSize) {
                remove();
            }
            return true;
        }

        public boolean checkNeedUpgradeMaster() {
            int size = size();
            if (size >= limitSize) {
                L previous = get(0);
                for (int i = 1; i < size; i++) {
                    L v = get(i);
                    if (!v.equals(previous)) {
                        return false;
                    } else {
                        previous = v;
                    }
                }
                return true;
            }
            return false;
        }
    }
}

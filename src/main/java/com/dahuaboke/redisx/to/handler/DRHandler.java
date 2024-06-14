package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 2024/6/12 17:10
 * auth: dahua
 * desc: 主备策略处理器
 */
public class DRHandler extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(DRHandler.class);
    private static final String lua = "local v = redis.call('GET',KEYS[1]);\n" +
            "    if v then\n" +
            "        return v;\n" +
            "    else\n" +
            "        local result = redis.call('SET',KEYS[1],ARGV[1]);\n" +
            "        return result;\n" +
            "    end";
    private ToContext toContext;
    //10次标志不变从升为主
    private final LimitedList<String> limitedList = new LimitedList(10);

    public DRHandler(ToContext toContext) {
        this.toContext = toContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Thread drThread = new Thread(() -> {
            while (!toContext.isClose()) {
                preemptMaster();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.error("DR thread interrupted");
                }
            }
        });
        drThread.setName(Constant.PROJECT_NAME + "-DRThread");
        drThread.start();
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, String reply) throws Exception {
        if (reply.startsWith(Constant.PROJECT_NAME)) {
            String[] split = reply.split("\\|");
            if (split.length != 4) {
                preemptMasterCompulsory();
            } else {
                if (!Constant.PROJECT_NAME.equals(split[0])) {
                    preemptMasterCompulsory();
                } else {
                    if (toContext.getId().equals(split[1])) { //主节点是自己
                        toContext.isMaster(true);
                        preemptMasterCompulsory();
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

    private void preemptMaster() {
        List<String> commands = new ArrayList() {{
            add("EVAL");
            add(lua);
            add("1");
            add(Constant.DR_KEY);
            add(preemptMasterCommand());
        }};
        toContext.sendCommand(commands, 1000);
    }

    private void preemptMasterCompulsory() {
        toContext.sendCommand("set " + Constant.DR_KEY + " " + preemptMasterCommand(), 1000);
    }

    private void preemptMasterCompulsory(String offset) {
        toContext.sendCommand("set " + Constant.DR_KEY + " " + preemptMasterCommand(), 1000);
    }

    private String preemptMasterCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constant.PROJECT_NAME);
        sb.append("|");
        sb.append(toContext.getId());
        sb.append("|");
        sb.append(toContext.getOffset());
        sb.append("|");
        sb.append(System.currentTimeMillis());
        return new String(sb);
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

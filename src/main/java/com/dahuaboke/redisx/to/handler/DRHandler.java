package com.dahuaboke.redisx.to.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.handler.RedisChannelInboundHandler;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/6/12 17:10
 * auth: dahua
 * desc:
 */
public class DRHandler extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(DRHandler.class);
    private static final String lua = "EVAL \"local v = redis.call('GET',KEYS[1]);\n" +
            "    if v then\n" +
            "        return v;\n" +
            "    else\n" +
            "        local result = redis.call('SET',KEYS[1],ARGV[1]);\n" +
            "        return result;\n" +
            "    end\" 1 " + Constant.DR_KEY + " ";
    private ToContext toContext;

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
            if (split.length != 3) {
                preemptMasterCompulsory();
            } else {
                if (!Constant.PROJECT_NAME.equals(split[0])) {
                    preemptMasterCompulsory();
                } else {
                    if (toContext.getId().equals(split[1])) { //主节点是自己
                        toContext.isMaster(true);
                    } else { //主节点非自己
                        toContext.isMaster(false);
                    }
                }
            }
        }
    }

    private void preemptMaster() {
        toContext.sendCommand(lua + preemptMasterCommand(), 10000);
    }

    private void preemptMasterCompulsory() {
        toContext.sendCommand("set " + Constant.DR_KEY + " " + preemptMasterCommand(), 10000);
    }

    private String preemptMasterCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(Constant.PROJECT_NAME);
        sb.append("|");
        sb.append(toContext.getId());
        sb.append("|");
        sb.append(System.currentTimeMillis());
        return new String(sb);
    }
}

package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Pattern;


/**
 * 2024/5/15 15:00
 * auth: cdl
 * desc:
 */
public class SlotInfoHandler extends RedisChannelInboundHandler {

    private static final Logger logger = LoggerFactory.getLogger(CommandEncoder.class);
    private Context context;

    public SlotInfoHandler(Context context) {
        super(context);
        this.context = context;
    }

    @Override
    public void channelRead1(ChannelHandlerContext ctx, String msg) throws Exception {
        if ("SLOTSEND".equals(msg)) {
            logger.info("Beginning send slot get command");
            Channel channel = ctx.channel();
            if (channel.isActive()) {
                //不需要去pipeline的底部，所以直接ctx.write
                ctx.writeAndFlush(Constant.GET_SLOT_COMMAND);
            }
        } else {
            logger.info("Beginning slot message parse");
            Pattern pattern = Pattern.compile(Constant.SLOT_REX, Pattern.DOTALL);
            if (msg != null && pattern.matcher(msg).matches()) {
                msg = msg.replace("\r", "");
                String[] arr = msg.split("\n");
                if (arr.length != 0) {
                    Arrays.stream(arr).forEach(s -> {
                        SlotInfo slotInfo = new SlotInfo(s);
                        if (context instanceof FromContext) {
                            FromContext fromContext = (FromContext) context;
                            if (fromContext.getHost().equals(slotInfo.getIp()) &&
                                    fromContext.getPort() == slotInfo.getPort()) {
                                fromContext.setSlotInfo(slotInfo);
                                fromContext.setSlotBegin(slotInfo.getSlotStart());
                                fromContext.setSlotEnd(slotInfo.getSlotEnd());
                                ctx.pipeline().remove(this);
                            }
                        } else if (context instanceof ToContext) {
                            ToContext toContext = (ToContext) context;
                            if (toContext.getHost().equals(slotInfo.getIp()) &&
                                    toContext.getPort() == slotInfo.getPort()) {
                                toContext.setSlotInfo(slotInfo);
                                toContext.setSlotBegin(slotInfo.getSlotStart());
                                toContext.setSlotEnd(slotInfo.getSlotEnd());
                                ctx.pipeline().remove(this);
                            }
                        }
                    });
                }
            }
        }
    }

    public class SlotInfo {
        //节点编号，40位随机字符串
        private String id;
        //节点IP
        private String ip;
        //节点端口
        private int port;
        //待确定作用
        private String nodeNum;
        //节点类型 master s
        private String flags;
        //主节点Id，只有当前节点是从节点才有值
        private String masterId;
        //最近一次发送ping的unix毫秒时间戳，0代表没有发送过
        private long pingSend;
        //最近一次收到pong的unix毫秒时间戳
        private long pingRecv;
        //该节点或其master节点的epoch值。每次故障转移都会生成一个新的，唯一的，递增的epoch值。若多个节点竞争相同的slot，epoch值大的获胜
        private int configEpoch;
        //节点和集群总线间的连接状态，可以是connected或disconnected
        private String linkState;
        //槽起始值
        private int slotStart;
        //槽终止值
        private int slotEnd;

        public SlotInfo(String msg) {
            String[] arr = msg.split(" ");
            this.id = arr[0];
            int a = arr[1].indexOf(":");
            int b = arr[1].indexOf("@");
            this.ip = arr[1].substring(0, a);
            if (b == -1) {
                port = Integer.parseInt(arr[1].substring(b + 1));
            } else {
                port = Integer.parseInt(arr[1].substring(a + 1, b));
                nodeNum = arr[1].substring(b + 1);
            }
            this.flags = arr[2].indexOf(",") == -1 ? arr[2] : arr[2].split(",")[1];
            this.masterId = arr[3];
            this.pingSend = Long.parseLong(arr[4]);
            this.pingRecv = Long.parseLong(arr[5]);
            this.configEpoch = Integer.parseInt(arr[6]);
            this.linkState = arr[7];
            if (arr.length > 8) {
                String[] slots = arr[8].split("-");
                this.slotStart = Integer.parseInt(slots[0]);
                this.slotEnd = Integer.parseInt(slots[1]);
            }
        }

        public String getIp() {
            return ip;
        }

        public int getPort() {
            return port;
        }

        public int getSlotStart() {
            return slotStart;
        }

        public int getSlotEnd() {
            return slotEnd;
        }
    }
}

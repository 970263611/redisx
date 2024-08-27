package com.dahuaboke.redisx.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.annotation.FieldOrm;
import com.dahuaboke.redisx.from.FromContext;
import com.dahuaboke.redisx.to.ToContext;
import com.dahuaboke.redisx.common.utils.FieldOrmUtil;
import com.dahuaboke.redisx.common.utils.StringUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        //sendMasterCommand(ctx);
        sendSlaveCommand(ctx);
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead2(ChannelHandlerContext ctx, String msg) throws Exception {
        //parseMasterMessage(ctx, msg);
        parseSlaveMessage(ctx, msg);
    }

    private void sendMasterCommand(ChannelHandlerContext ctx) {
        logger.info("Beginning sentinel get master command");
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            //不需要去pipeline的底部，所以直接ctx.write
            ctx.writeAndFlush(Constants.SENTINEL_GET_MASTER + masterName);
        }
    }

    private void sendSlaveCommand(ChannelHandlerContext ctx) {
        logger.info("Beginning sentinel get master command");
        Channel channel = ctx.channel();
        if (channel.isActive()) {
            //不需要去pipeline的底部，所以直接ctx.write
            ctx.writeAndFlush(Constants.SENTINEL_GET_SLAVE + masterName);
        }
    }

    private void parseMasterMessage(ChannelHandlerContext ctx, String msg) {
        logger.info("Beginning sentinel master message parse");
        if (StringUtils.isNotEmpty(msg)) {
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

    private void parseSlaveMessage(ChannelHandlerContext ctx, String msg) {
        logger.info("Beginning sentinel slave message parse");
        List<SlaveInfo> slaveInfos = new ArrayList<>();//这个就是解析后的从节点集合
        String[] arrs = msg.split(" ");
        Map<String, Object> map = new HashMap<>();
        String key = null;
        for (int i = 0; i < arrs.length; i++) {
            if (i % 2 == 0) {
                key = arrs[i];
                if (map.containsKey(key)) {
                    SlaveInfo slaveInfo = new SlaveInfo();
                    FieldOrmUtil.MapToBean(map, slaveInfo);
                    slaveInfos.add(slaveInfo);
                    map.clear();
                }
            } else {
                map.put(key, arrs[i]);
                if (i == arrs.length - 1) {
                    SlaveInfo slaveInfo = new SlaveInfo();
                    FieldOrmUtil.MapToBean(map, slaveInfo);
                    slaveInfos.add(slaveInfo);
                    map.clear();
                }
            }
        }
        if (context instanceof FromContext) {
            FromContext fromContext = (FromContext) context;
            for (SlaveInfo slaveInfo : slaveInfos) {
                fromContext.addSentinelSlaveInfo(slaveInfo);
            }
            fromContext.setFromNodesInfoGetSuccess();
        } else if (context instanceof ToContext) {
            ToContext toContext = (ToContext) context;
            for (SlaveInfo slaveInfo : slaveInfos) {
                if (slaveInfo.isActive()) {
                    toContext.setSentinelMasterInfo(slaveInfo.getMasterHost(), slaveInfo.getMasterPort());
                    break;
                }
            }
            toContext.setToNodesInfoGetSuccess();
        }
    }

    public class SlaveInfo {

        @FieldOrm("name")
        private String name;

        @FieldOrm("ip")
        private String ip;

        @FieldOrm("port")
        private int port;

        @FieldOrm("runid")
        private String runid;

        @FieldOrm("flags")
        private String flags;

        @FieldOrm("link-pending-commands")
        private String linkPendingCommands;

        @FieldOrm("link-refcount")
        private String linkRefcount;

        @FieldOrm("last-ping-sent")
        private String lastPingSent;

        @FieldOrm("last-ok-ping-reply")
        private String lastOkPingReply;

        @FieldOrm("last-ping-reply")
        private String lastPingReply;

        @FieldOrm("down-after-milliseconds")
        private long downAfterMilliseconds;

        @FieldOrm("info-refresh")
        private String infoRefresh;

        @FieldOrm("role-reported")
        private String roleReported;

        @FieldOrm("role-reported-time")
        private long roleReportedTime;

        @FieldOrm("master-link-down-time")
        private String masterLinkDownTime;

        @FieldOrm("master-link-status")
        private String masterLinkStatus;

        @FieldOrm("master-host")
        private String masterHost;

        @FieldOrm("master-port")
        private int masterPort;

        @FieldOrm("slave-priority")
        private String SlavePriority;

        @FieldOrm("slave-repl-offset")
        private String SlaveReplOffset;

        @FieldOrm("replica-announced")
        private String replicaAnnounced;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getRunid() {
            return runid;
        }

        public void setRunid(String runid) {
            this.runid = runid;
        }

        public String getFlags() {
            return flags;
        }

        public void setFlags(String flags) {
            this.flags = flags;
        }

        public String getLinkPendingCommands() {
            return linkPendingCommands;
        }

        public void setLinkPendingCommands(String linkPendingCommands) {
            this.linkPendingCommands = linkPendingCommands;
        }

        public String getLinkRefcount() {
            return linkRefcount;
        }

        public void setLinkRefcount(String linkRefcount) {
            this.linkRefcount = linkRefcount;
        }

        public String getLastPingSent() {
            return lastPingSent;
        }

        public void setLastPingSent(String lastPingSent) {
            this.lastPingSent = lastPingSent;
        }

        public String getLastOkPingReply() {
            return lastOkPingReply;
        }

        public void setLastOkPingReply(String lastOkPingReply) {
            this.lastOkPingReply = lastOkPingReply;
        }

        public String getLastPingReply() {
            return lastPingReply;
        }

        public void setLastPingReply(String lastPingReply) {
            this.lastPingReply = lastPingReply;
        }

        public long getDownAfterMilliseconds() {
            return downAfterMilliseconds;
        }

        public void setDownAfterMilliseconds(long downAfterMilliseconds) {
            this.downAfterMilliseconds = downAfterMilliseconds;
        }

        public String getInfoRefresh() {
            return infoRefresh;
        }

        public void setInfoRefresh(String infoRefresh) {
            this.infoRefresh = infoRefresh;
        }

        public String getRoleReported() {
            return roleReported;
        }

        public void setRoleReported(String roleReported) {
            this.roleReported = roleReported;
        }

        public long getRoleReportedTime() {
            return roleReportedTime;
        }

        public void setRoleReportedTime(long roleReportedTime) {
            this.roleReportedTime = roleReportedTime;
        }

        public String getMasterLinkDownTime() {
            return masterLinkDownTime;
        }

        public void setMasterLinkDownTime(String masterLinkDownTime) {
            this.masterLinkDownTime = masterLinkDownTime;
        }

        public String getMasterLinkStatus() {
            return masterLinkStatus;
        }

        public void setMasterLinkStatus(String masterLinkStatus) {
            this.masterLinkStatus = masterLinkStatus;
        }

        public String getMasterHost() {
            return masterHost;
        }

        public void setMasterHost(String masterHost) {
            this.masterHost = masterHost;
        }

        public int getMasterPort() {
            return masterPort;
        }

        public void setMasterPort(int masterPort) {
            this.masterPort = masterPort;
        }

        public String getSlavePriority() {
            return SlavePriority;
        }

        public void setSlavePriority(String slavePriority) {
            SlavePriority = slavePriority;
        }

        public String getSlaveReplOffset() {
            return SlaveReplOffset;
        }

        public void setSlaveReplOffset(String slaveReplOffset) {
            SlaveReplOffset = slaveReplOffset;
        }

        public String getReplicaAnnounced() {
            return replicaAnnounced;
        }

        public void setReplicaAnnounced(String replicaAnnounced) {
            this.replicaAnnounced = replicaAnnounced;
        }

        public boolean isActive() {
            return flags.equals("slave");
        }
    }
}

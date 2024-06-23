package com.dahuaboke.redisx.from;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.cache.CacheManager;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * 2024/5/6 16:18
 * auth: dahua
 * desc: 从节点上下文
 */
public class FromContext extends Context {

    private static final Logger logger = LoggerFactory.getLogger(FromContext.class);
    private CacheManager cacheManager;
    private String host;
    private int port;
    private Channel fromChannel;
    private String localHost;
    private int localPort;
    private int slotBegin;
    private int slotEnd;
    private FromClient fromClient;
    private boolean isConsole;
    private boolean fromIsCluster;
    private boolean rdbAckOffset = false;
    private int db = -1;
    private int pingSize = 0;

    public FromContext(CacheManager cacheManager, String host, int port, boolean isConsole, boolean fromIsCluster) {
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.isConsole = isConsole;
        if (isConsole) {
            replyQueue = new LinkedBlockingDeque();
        }
        this.fromIsCluster = fromIsCluster;
    }

    public String getId() {
        return cacheManager.getId();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean publish(String msg, Integer length) {
        if (!isConsole) {
            if (msg.toUpperCase().startsWith(Constant.SELECT_PREFIX)) {
                String dbStr = msg.substring(Constant.SELECT_PREFIX.length());
                try {
                    setDb(Integer.parseInt(dbStr));
                    logger.debug("Change db to [{}]", dbStr);
                } catch (NumberFormatException e) {
                    return false;
                }
                return true;
            } else if (msg.equalsIgnoreCase(Constant.PING_COMMAND)) {
                pingSize++;
                logger.trace("Receive ping command, having ping size [{}]", pingSize);
                return true;
            } else {
                boolean successPublish = cacheManager.publish(msg, length, this, pingSize);
                if (pingSize > 0) {
                    clearPingSize();
                }
                return successPublish;
            }
        } else {
            if (replyQueue == null) {
                throw new IllegalStateException("By console mode replyQueue need init");
            } else {
                return replyQueue.offer(msg);
            }
        }
    }

    public void setFromChannel(Channel fromChannel) {
        this.fromChannel = fromChannel;
        InetSocketAddress inetSocketAddress = (InetSocketAddress) this.fromChannel.localAddress();
        this.localHost = inetSocketAddress.getHostString();
        this.localPort = inetSocketAddress.getPort();
    }

    public String getLocalHost() {
        return localHost;
    }

    public int getLocalPort() {
        return localPort;
    }

    public void setFromClient(FromClient fromClient) {
        this.fromClient = fromClient;
    }

    @Override
    public boolean isAdapt(boolean isMasterCluster, String command) {
        if (isMasterCluster && command != null) {
            int hash = calculateHash(command) % Constant.COUNT_SLOT_NUMS;
            return hash >= slotBegin && hash <= slotEnd;
        } else {
            //哨兵模式或者单节点则只存在一个为ToContext类型的context
            return true;
        }
    }

    public boolean isConsole() {
        return isConsole;
    }

    public void setSlotBegin(int slotBegin) {
        this.slotBegin = slotBegin;
    }

    public void setSlotEnd(int slotEnd) {
        this.slotEnd = slotEnd;
    }

    public boolean isFromIsCluster() {
        return fromIsCluster;
    }

    @Override
    public String sendCommand(Object command, int timeout) {
        if (replyQueue == null) {
            throw new IllegalStateException("By console mode replyQueue need init");
        } else {
            replyQueue.clear();
            fromClient.sendCommand((String) command);
            try {
                return replyQueue.poll(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return null;
            }
        }
    }

    public void close() {
        this.fromClient.destroy();
        cacheManager.remove(this);
    }

    public long getOffset() {
        return cacheManager.getNodeMessage(host, port).getOffset();
    }

    public void setOffset(long offset) {
        String masterId = this.fromChannel.attr(Constant.MASTER_ID).get();
        cacheManager.setNodeMessage(this.host, this.port, masterId, offset);
    }

    public CacheManager.NodeMessage getNodeMessage() {
        return cacheManager.getNodeMessage(this.host, this.port);
    }

    public void ackOffset() {
        if (fromChannel != null && fromChannel.isActive() && fromChannel.pipeline().get(Constant.INIT_SYNC_HANDLER_NAME) == null) {
            Long offsetSession = fromChannel.attr(Constant.OFFSET).get();
            CacheManager.NodeMessage nodeMessage = getNodeMessage();
            if (offsetSession != null && offsetSession > -1L) {
                if (nodeMessage == null) {
                    setOffset(offsetSession);
                }
                fromChannel.attr(Constant.OFFSET).set(-1L);
            }
            if (nodeMessage != null) {
                long offset = getOffset();
                fromChannel.writeAndFlush(Constant.ACK_COMMAND_PREFIX + offset);
                logger.trace("Ack offset [{}]", offset);
            }
        }
    }

    public boolean isRdbAckOffset() {
        return rdbAckOffset;
    }

    public void setRdbAckOffset(boolean rdbAckOffset) {
        this.rdbAckOffset = rdbAckOffset;
    }

    public String getPassword() {
        return cacheManager.getFromPassword();
    }

    public int getDb() {
        return db;
    }

    public void setDb(int db) {
        this.db = db;
    }

    public void clearPingSize() {
        this.pingSize = 0;
    }
}

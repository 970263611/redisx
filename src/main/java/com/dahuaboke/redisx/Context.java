package com.dahuaboke.redisx;

import com.dahuaboke.redisx.cache.CacheManager;
import com.dahuaboke.redisx.handler.SlotInfoHandler;
import com.dahuaboke.redisx.utils.CRC16;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingDeque;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    protected BlockingDeque<String> replyQueue;
    protected SlotInfoHandler.SlotInfo slotInfo;
    protected boolean isClose = false;
    protected boolean isConsole;

    public boolean isAdapt(boolean isCluster, String command) {
        return false;
    }

    public String sendCommand(String command, int timeout) {
        throw new RuntimeException();
    }

    public void setSlotInfo(SlotInfoHandler.SlotInfo slotInfo) {
        this.slotInfo = slotInfo;
    }

    protected int calculateHash(String command) {
        String[] ary = command.split(" ");
        if (ary.length > 1) {
            return CRC16.crc16(ary[1].getBytes(StandardCharsets.UTF_8));
        } else {
            logger.warn("Command split length should > 1");
            return 0;
        }
    }

    public boolean isClose() {
        return isClose;
    }

    public void setClose(boolean close) {
        isClose = close;
    }
}

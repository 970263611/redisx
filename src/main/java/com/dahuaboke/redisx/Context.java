package com.dahuaboke.redisx;

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
    protected boolean toIsCluster;
    protected boolean fromIsCluster;

    public Context(boolean fromIsCluster, boolean toIsCluster) {
        this.fromIsCluster = fromIsCluster;
        this.toIsCluster = toIsCluster;
    }

    public boolean isAdapt(boolean isCluster, String command) {
        return false;
    }

    public String sendCommand(Object command, int timeout) {
        throw new RuntimeException();
    }

    public void setSlotInfo(SlotInfoHandler.SlotInfo slotInfo) {
        this.slotInfo = slotInfo;
    }

    protected int calculateHash(String command) {
        if(command == null || command.length() == 0){
            return -1;
        }
        int leftIdx = command.indexOf("{");
        if(leftIdx != -1 && leftIdx < (command.length() - 1)){//存在左括号 且 左括号位置不是最后一个
            int rightIdx = command.indexOf("}",leftIdx + 1);//在第一个左括号的右侧找第一个右括号
            if(rightIdx != -1 && leftIdx < (rightIdx - 1)){//存在右括号 且 左右括号之间必须有内容
                command = command.substring(leftIdx + 1,rightIdx);
            }
        }
        return CRC16.crc16(command.getBytes());
    }

    public boolean isClose() {
        return isClose;
    }

    public void setClose(boolean close) {
        isClose = close;
    }

    public boolean isToIsCluster() {
        return toIsCluster;
    }

    public boolean isFromIsCluster() {
        return fromIsCluster;
    }

    public boolean isConsole() {
        return isConsole;
    }
}

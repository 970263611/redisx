package com.dahuaboke.redisx;

import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.utils.CRC16;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;

/**
 * 2024/5/13 11:09
 * auth: dahua
 * desc:
 */
public class Context {

    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    protected BlockingDeque<String> replyQueue;
    protected boolean isClose = false;
    protected boolean consoleStart;
    protected CacheManager cacheManager;
    protected String host;
    protected int port;
    protected Mode toMode;
    protected Mode fromMode;

    public Context(CacheManager cacheManager, String host, int port, Mode fromMode, Mode toMode, boolean consoleStart) {
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.fromMode = fromMode;
        this.toMode = toMode;
        this.consoleStart = consoleStart;
    }

    public boolean isAdapt(Mode mode, String command) {
        return false;
    }

    public String sendCommand(Object command, int timeout) {
        throw new RuntimeException();
    }

    protected int calculateHash(String command) {
        if (command == null || command.length() == 0) {
            return -1;
        }
        int leftIdx = command.indexOf("{");
        if (leftIdx != -1 && leftIdx < (command.length() - 1)) {//存在左括号 且 左括号位置不是最后一个
            int rightIdx = command.indexOf("}", leftIdx + 1);//在第一个左括号的右侧找第一个右括号
            if (rightIdx != -1 && leftIdx < (rightIdx - 1)) {//存在右括号 且 左右括号之间必须有内容
                command = command.substring(leftIdx + 1, rightIdx);
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

    public Mode getToMode() {
        return toMode;
    }

    public Mode getFromMode() {
        return fromMode;
    }

    public boolean isConsoleStart() {
        return consoleStart;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}

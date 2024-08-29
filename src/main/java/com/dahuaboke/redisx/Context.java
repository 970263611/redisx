package com.dahuaboke.redisx;

import com.dahuaboke.redisx.common.cache.CacheManager;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.utils.CRC16;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

    public boolean isAdapt(Mode mode, byte[] command) {
        return false;
    }

    public boolean isAdapt(Mode mode, String command) {
        return isAdapt(mode, command.getBytes());
    }

    public String sendCommand(Object command, int timeout) {
        throw new RuntimeException();
    }

    protected int calculateHash(byte[] command) {
        if (command == null || command.length == 0) {
            return -1;
        }
        List<Byte> keyByte = new ArrayList<Byte>();
        boolean startFlag = false;
        boolean endFlag = false;
        for (byte b : command) {
            if (b == '{') {
                startFlag = true;
                continue;
            }
            if (b == '}') {
                endFlag = true;
                break;
            }
            if (startFlag) {
                keyByte.add(b);
            }
        }
        if (startFlag && endFlag && keyByte.size() > 0) {
            byte[] arrs = new byte[keyByte.size()];
            for (int i = 0; i < keyByte.size(); i++) {
                arrs[i] = keyByte.get(i);
            }
            return CRC16.crc16(arrs);
        } else {
            return CRC16.crc16(command);
        }
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

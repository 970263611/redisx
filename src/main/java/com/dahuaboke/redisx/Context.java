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
    protected boolean startConsole;
    protected boolean startByConsole;
    protected CacheManager cacheManager;
    protected String host;
    protected int port;
    protected Mode toMode;
    protected Mode fromMode;
    protected Long writeCount = 0L;
    protected Long errorCount = 0L;

    public Context(CacheManager cacheManager, String host, int port, Mode fromMode, Mode toMode, boolean startConsole, boolean startByConsole) {
        this.cacheManager = cacheManager;
        this.host = host;
        this.port = port;
        this.fromMode = fromMode;
        this.toMode = toMode;
        this.startConsole = startConsole;
        this.startByConsole = startByConsole;
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

    public boolean startByConsole() {
        return startByConsole;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void addWriteCount() {
        this.writeCount++;
        if (this.writeCount == Long.MAX_VALUE) {
            this.writeCount = 0L;
        }
    }

    public Long getWriteCount() {
        return writeCount;
    }

    public void addErrorCount() {
        this.errorCount++;
        if (this.errorCount == Long.MAX_VALUE) {
            this.errorCount = 0L;
        }
        cacheManager.setErrorCount(host, port, this.errorCount);
    }

    public Long getErrorCount() {
        return cacheManager.getErrorCount(host, port);
    }

    public boolean isStartConsole() {
        return startConsole;
    }
}

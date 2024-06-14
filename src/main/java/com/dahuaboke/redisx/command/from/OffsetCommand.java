package com.dahuaboke.redisx.command.from;

import com.dahuaboke.redisx.Constant;

/**
 * 2024/5/8 9:33
 * auth: dahua
 * desc:
 */
public class OffsetCommand {

    private String masterId;
    private long offset;

    public OffsetCommand(String command) {
        String[] s = command.split(" ");
        masterId = s[1];
        //TODO 未找到这个 +1 偏移量的原因，观察发现差1
        offset = Long.parseLong(s[2]) + 1;
    }

    public String getMasterId() {
        return masterId;
    }

    public long getOffset() {
        return offset;
    }
}

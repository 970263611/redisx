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
        offset = Long.parseLong(s[2]);
    }

    public String getMasterId() {
        return masterId;
    }

    public long getOffset() {
        return offset;
    }
}

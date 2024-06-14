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
        if (command.startsWith(Constant.FULLRESYNC)) {
            masterId = s[1];
            offset = Long.parseLong(s[2]);
        } else if (command.startsWith(Constant.CONTINUE)) {
            if (s.length > 2) {
                masterId = s[1];
                offset = Long.parseLong(s[2]);
            } else {
                try {
                    offset = Long.parseLong(s[1]);
                } catch (NumberFormatException e) {
                    masterId = s[1];
                }
            }
        }
    }

    public String getMasterId() {
        return masterId;
    }

    public long getOffset() {
        return offset;
    }
}

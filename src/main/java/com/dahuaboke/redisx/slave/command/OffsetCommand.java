package com.dahuaboke.redisx.slave.command;

import com.dahuaboke.redisx.slave.SlaveConst;

/**
 * 2024/5/8 9:33
 * auth: dahua
 * desc:
 */
public class OffsetCommand extends SystemCommand {

    private String masterId;
    private Long offset;

    public OffsetCommand(String command) {
        String[] s = command.split(" ");
        if (command.startsWith(SlaveConst.FULLRESYNC)) {
            masterId = s[1];
            offset = Long.parseLong(s[2]);
        } else if (command.startsWith(SlaveConst.CONTINUE)) {
            offset = Long.parseLong(s[1]);
        }
    }

    public String getMasterId() {
        return masterId;
    }

    public Long getOffset() {
        return offset;
    }
}

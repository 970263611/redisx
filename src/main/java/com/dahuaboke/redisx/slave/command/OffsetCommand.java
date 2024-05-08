package com.dahuaboke.redisx.slave.command;

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
        masterId = s[1];
        offset = Long.parseLong(s[2]);
    }

    public String getMasterId() {
        return masterId;
    }

    public Long getOffset() {
        return offset;
    }
}

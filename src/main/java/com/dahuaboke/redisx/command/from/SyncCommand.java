package com.dahuaboke.redisx.command.from;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.command.Command;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand extends Command {

    private List<String> command = new LinkedList<>();
    private Context context;
    private int length = 0;
    private int syncLength;
    private boolean needAddLengthToOffset;
    private static List<String> specialCommandPrefix = new ArrayList<String>() {{
        add("BITOP");
        add("MEMORY");
        add("BZMPOP");
        add("OBJECT");
        add("EVAL");
        add("EVAL_RO");
        add("EVALSHA");
        add("EVALSHA_RO");
        add("FCALL");
        add("FCALL_RO");
        add("PFDEBUG");
        add("XGROUP");
        add("XINFO");
    }};

    public SyncCommand(Context context, boolean needAddLengthToOffset) {
        this.context = context;
        this.needAddLengthToOffset = needAddLengthToOffset;
    }

    public SyncCommand(Context context, List<String> command, boolean needAddLengthToOffset) {
        this.context = context;
        this.command = command;
        this.needAddLengthToOffset = needAddLengthToOffset;
    }

    public List<String> getCommand() {
        return command;
    }

    public int getSyncLength() {
        return syncLength;
    }

    public void setSyncLength(int syncLength) {
        this.syncLength = syncLength;
    }

    public int getCommandLength() {
        return length;
    }

    public void appendCommand(String c) {
        command.add(c);
    }

    public void appendLength(int l) {
        length += l;
    }

    public String getStringCommand() {
        StringBuilder sb = new StringBuilder();
        for (String s : command) {
            if (sb.length() != 0) {
                sb.append(" ");
            }
            sb.append(s);
        }
        return new String(sb);
    }

    public boolean isIgnore() {
        String stringCommand = getStringCommand();
        if (stringCommand.toUpperCase().startsWith(Constant.SELECT)) {
            return context.isFromIsCluster() || context.isToIsCluster();
        }
        return Constant.PING_COMMAND.equalsIgnoreCase(stringCommand)
                || Constant.MULTI.equalsIgnoreCase(stringCommand)
                || Constant.EXEC.equalsIgnoreCase(stringCommand);
    }

    public String getKey() {
        try {
            String s = command.get(0);
            if (specialCommandPrefix.contains(s.toUpperCase())) {
                return command.get(2);
            }
            return command.get(1);
        } catch (Exception e) {
            throw new RuntimeException(command.toString());
        }
    }

    public boolean isNeedAddLengthToOffset() {
        return needAddLengthToOffset;
    }

    public Context getContext() {
        return context;
    }

    @Override
    public String toString() {
        return command.toString();
    }
}

package com.dahuaboke.redisx.common.command.from;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.command.Command;
import com.dahuaboke.redisx.common.enums.Mode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * 2024/5/9 9:37
 * auth: dahua
 * desc:
 */
public class SyncCommand extends Command {

    private static final Logger logger = LoggerFactory.getLogger(SyncCommand.class);
    private List<String> command = new LinkedList<>();
    private Context context;
    private int length = 0;
    private int syncLength;
    private boolean needAddLengthToOffset;
    private RedisMessage redisMessage;
    private String stringCommand;
    String key;
    private static List<String> specialCommandPrefix = new ArrayList<String>() {{
        add("BITOP");
        add("MEMORY");
        add("BZMPOP");
        add("OBJECT");
//        add("EVAL");
//        add("EVAL_RO");
//        add("EVALSHA");
//        add("EVALSHA_RO");
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

    public RedisMessage getCommand() {
        return redisMessage;
    }

    public void buildCommand() {
        List<RedisMessage> children = new LinkedList<>();
        for (String c : command) {
            ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
            buffer.writeBytes(c.getBytes());
            children.add(new FullBulkStringRedisMessage(buffer));
        }
        redisMessage = new ArrayRedisMessage(children);
        String s = command.get(0);
        if (command.size() > 1) {
            if (specialCommandPrefix.contains(s.toUpperCase())) {
                key = command.get(2);
            } else {
                key = command.get(1);
            }
        } else {
            logger.warn("Command not has key [{}]", command);
            key = s;
        }
        getStringCommand();
        command = null;
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
        if (stringCommand == null) {
            StringBuilder sb = new StringBuilder();
            for (String s : command) {
                if (sb.length() != 0) {
                    sb.append(" ");
                }
                sb.append(s);
            }
            stringCommand = new String(sb);
        }
        return stringCommand;
    }

    public boolean isIgnore() {
        String stringCommand = getStringCommand();
        if (stringCommand.toUpperCase().startsWith(Constants.SELECT)) {
            return Mode.CLUSTER == context.getFromMode() || Mode.CLUSTER == context.getToMode();
        }
        if (stringCommand.toUpperCase().startsWith(Constants.EVAL)) {
            return Mode.CLUSTER == context.getToMode();
        }
        if (stringCommand.toUpperCase().startsWith(Constants.PUBLISH)) {
            return Mode.SENTINEL == context.getFromMode();
        }
        return Constants.PING_COMMAND.equalsIgnoreCase(stringCommand) || Constants.MULTI.equalsIgnoreCase(stringCommand) || Constants.EXEC.equalsIgnoreCase(stringCommand);
    }

    public String getKey() {
        return key;
    }

    public boolean isNeedAddLengthToOffset() {
        return needAddLengthToOffset;
    }

    public Context getContext() {
        return context;
    }
}

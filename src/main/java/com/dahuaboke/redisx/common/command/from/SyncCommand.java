package com.dahuaboke.redisx.common.command.from;

import com.dahuaboke.redisx.Context;
import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.Command;
import com.dahuaboke.redisx.common.enums.Mode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
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
    private ArrayRedisMessage redisMessage;
    private String stringCommand;
    private byte[] key;

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

    public SyncCommand(Context context, List<byte[]> command, boolean needAddLengthToOffset) {
        this.context = context;
        List<RedisMessage> children = new LinkedList<>();
        for (byte[] commandBytes : command) {
            this.command.add(new String(commandBytes, Charset.defaultCharset()));
            ByteBuf buffer = Unpooled.buffer();
            buffer.writeBytes(commandBytes);
            children.add(new FullBulkStringRedisMessage(buffer));
        }
        this.redisMessage = new ArrayRedisMessage(children);
        this.needAddLengthToOffset = needAddLengthToOffset;
    }

    public RedisMessage getCommand() {
        return redisMessage;
    }

    public void buildCommand() {
        getStringCommand();
        command = null;
        boolean keyFlag = false;
        if (redisMessage != null && redisMessage.children().size() > 1 && redisMessage.children().get(0) instanceof FullBulkStringRedisMessage) {
            List<RedisMessage> children = redisMessage.children();
            FullBulkStringRedisMessage rm0 = (FullBulkStringRedisMessage) children.get(0);
            String redisCommand = rm0.content().toString(Charset.defaultCharset());
            if (specialCommandPrefix.contains(redisCommand)) {
                if (children.size() > 2 && children.get(2) instanceof FullBulkStringRedisMessage) {
                    FullBulkStringRedisMessage rm2 = (FullBulkStringRedisMessage) children.get(2);
                    key = new byte[rm2.content().readableBytes()];
                    rm2.content().getBytes(0, key);
                    keyFlag = true;
                }
            } else {
                if (children.get(1) instanceof FullBulkStringRedisMessage) {
                    FullBulkStringRedisMessage rm1 = (FullBulkStringRedisMessage) children.get(1);
                    key = new byte[rm1.content().readableBytes()];
                    rm1.content().getBytes(0, key);
                    keyFlag = true;
                }
            }
            if (!keyFlag) {
                key = new byte[rm0.content().readableBytes()];
                rm0.content().getBytes(0, key);
            }
        }
        if (!keyFlag) {
            logger.warn("Command not has key [{}]", command);
        }
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

    public byte[] getKey() {
        return key;
    }

    public boolean isNeedAddLengthToOffset() {
        return needAddLengthToOffset;
    }

    public Context getContext() {
        return context;
    }

    public void setRedisMessage(RedisMessage redisMessage) {
        if (redisMessage instanceof ArrayRedisMessage) {
            ArrayRedisMessage arrayRedisMessage = (ArrayRedisMessage) redisMessage;
            List<RedisMessage> children = new LinkedList<>();
            for (RedisMessage childMessage : arrayRedisMessage.children()) {
                if (childMessage instanceof FullBulkStringRedisMessage) {
                    FullBulkStringRedisMessage fullBulkStringRedisMessage = (FullBulkStringRedisMessage) childMessage;
                    ByteBuf buffer = Unpooled.buffer();
                    fullBulkStringRedisMessage.content().markReaderIndex();
                    buffer.writeBytes(fullBulkStringRedisMessage.content());
                    fullBulkStringRedisMessage.content().resetReaderIndex();
                    children.add(new FullBulkStringRedisMessage(buffer));
                } else {
                    children.add(childMessage);
                }
            }
            this.redisMessage = new ArrayRedisMessage(children);
        }
    }
}

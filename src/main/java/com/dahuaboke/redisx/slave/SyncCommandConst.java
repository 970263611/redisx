package com.dahuaboke.redisx.slave;

import io.netty.util.AttributeKey;

/**
 * 2024/5/8 9:14
 * auth: dahua
 * desc:
 */
public class SyncCommandConst {

    public static final String PROJECT_NAME = "REDISX";

    public static final String PING = "+PING";

    public static final String PING_COMMAND = "PING";

    public static final String PONG = "+PONG";

    public static final String PONG_COMMAND = "PONG";

    public static final String FULLRESYNC = "+FULLRESYNC";

    public static final AttributeKey<Long> OFFSET = AttributeKey.valueOf("offset");

    public static final AttributeKey<String> SYNC_REPLY = AttributeKey.valueOf("syncReply");

    public static final AttributeKey<Boolean> RDB_STREAM_NEXT = AttributeKey.valueOf("rdbStreamNext");

    public static final String INIT_SYNC_HANDLER_NAME = "INIT_SYNC_HANDLER";

}

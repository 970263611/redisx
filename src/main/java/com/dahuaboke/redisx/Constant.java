package com.dahuaboke.redisx;

import io.netty.util.AttributeKey;

/**
 * 2024/5/8 9:14
 * auth: dahua
 * desc:
 */
public class Constant {

    public static final String PROJECT_NAME = "Redis-x";

    public static final String PING_COMMAND = "PING";

    public static final String PONG_COMMAND = "PONG";

    public static final String OK_COMMAND = "OK";

    public static final String CONTINUE = "+CONTINUE";

    public static final String FULLRESYNC = "+FULLRESYNC";

    public static final AttributeKey<String> MASTER_ID = AttributeKey.valueOf("masterId");

    public static final AttributeKey<Long> OFFSET = AttributeKey.valueOf("offset");

    public static final AttributeKey<String> SYNC_REPLY = AttributeKey.valueOf("syncReply");

    public static final AttributeKey<Boolean> RDB_STREAM_NEXT = AttributeKey.valueOf("rdbStreamNext");

    public static final String INIT_SYNC_HANDLER_NAME = "INIT_SYNC_HANDLER";

    public static final String SLOT_HANDLER_NAME = "SLOT_HANDLER";

    public static final String ACK_COMMAND_PREFIX = "REPLCONF ack ";

    public static final String CONFIG_PORT_COMMAND_PREFIX = "REPLCONF listening-port ";

    public static final String CONFIG_HOST_COMMAND_PREFIX = "REPLCONF ip-address ";

    public static final String CONFIG_CAPA_COMMAND = "REPLCONF capa eof";

    public static final String CONFIG_ALL_PSYNC_COMMAND = "PSYNC ? -1";

    public static final String CONSOLE_URI_PREFIX = "/console";

    public static final String CONSOLE_COMMAND = "command";

    public static final String CONSOLE_TYPE = "type";

    public static final String NODE_MASTER = "master";

    public static final String NODE_SLAVE = "slave";

    public final static String GET_SLOT_COMMAND = "CLUSTER NODES";

    public final static String SLOT_REX = "^[0-9a-z]{40}.*";

}

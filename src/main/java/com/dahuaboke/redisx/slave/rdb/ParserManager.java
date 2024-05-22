package com.dahuaboke.redisx.slave.rdb;

import com.dahuaboke.redisx.slave.rdb.base.Parser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 2024/5/17 15:15
 * auth: dahua
 * desc:
 */
public class ParserManager {

    private static final Logger logger = LoggerFactory.getLogger(ParserManager.class);

    public static final Parser<byte[]> STRING_00 = new StringParser();

    public static final Parser<Map<byte[], byte[]>> ZIP_MAP = new StringParser();
}

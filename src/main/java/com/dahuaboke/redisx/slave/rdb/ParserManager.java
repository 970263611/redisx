package com.dahuaboke.redisx.slave.rdb;

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

    public final Parser<byte[]> STRING = new StringParser();

    public final Parser<Map<byte[], byte[]>> ZIP_MAP = new StringParser();
}

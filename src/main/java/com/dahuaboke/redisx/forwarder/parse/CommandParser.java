package com.dahuaboke.redisx.forwarder.parse;

import com.dahuaboke.redisx.command.forwarder.ForwardCommand;

/**
 * 2024/5/10 17:19
 * auth: dahua
 * desc:
 */
public interface CommandParser {

    ForwardCommand parse(String command);
}

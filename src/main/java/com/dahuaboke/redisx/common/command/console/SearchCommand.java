package com.dahuaboke.redisx.common.command.console;

import com.dahuaboke.redisx.common.command.Command;

/**
 * author: dahua
 * date: 2024/8/24 13:58
 */
public class SearchCommand extends Command {

    private String[] params;

    public SearchCommand(String[] params) {
        this.params = params;
    }

    public String[] getParams() {
        return params;
    }
}

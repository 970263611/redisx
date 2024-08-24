package com.dahuaboke.redisx.console.handler;

import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.command.console.ReplyCommand;
import com.dahuaboke.redisx.common.command.console.SearchCommand;
import com.dahuaboke.redisx.console.ConsoleContext;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * author: dahua
 * date: 2024/8/24 13:57
 */
public class SearchHandler extends SimpleChannelInboundHandler<SearchCommand> {

    private ConsoleContext consoleContext;

    public SearchHandler(ConsoleContext consoleContext) {
        this.consoleContext = consoleContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SearchCommand searchCommand) throws Exception {
        String command = null;
        String type = null;
        String reply = null;
        String params[] = searchCommand.getParams();
        if (params.length == 2) {
            String[] commandAndType = params[1].split("&");
            if (commandAndType.length == 2) {
                String[] commandKAndV = commandAndType[0].split("=");
                if (commandKAndV.length == 2 && Constants.CONSOLE_COMMAND.equalsIgnoreCase(commandKAndV[0])) {
                    command = commandKAndV[1];
                }
                String[] typeKAndV = commandAndType[1].split("=");
                if (typeKAndV.length == 2 && Constants.CONSOLE_TYPE.equalsIgnoreCase(typeKAndV[0])) {
                    type = typeKAndV[1];
                }
            }
        }
        if (command != null && type != null) {
            reply = consoleContext.sendCommand(command, type);
        }
        if (reply == null) {
            reply = "Send command error or request param error, uri should be [/console?command=xxx&type=left/right]";
        } else {
            StringBuilder sb = new StringBuilder();
            for (String s : reply.split(" ")) {
                if (sb.length() != 0) {
                    sb.append("\r\n");
                }
                sb.append(s);
            }
            reply = new String(sb);
        }
        ctx.fireChannelRead(new ReplyCommand(reply));
    }
}

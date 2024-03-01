package com.dahuaboke.redisx.netty.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author: dahua
 * date: 2024/2/28 11:03
 */
public class HttpRequestParamParser extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof FullHttpRequest) {
            FullHttpRequest request = (FullHttpRequest) msg;
            URI uri = new URI(request.uri());
            if ("/favicon.ico".equals(uri.getPath())) {
                return;
            }
            Map<String, String> parse = parse(request);
            ctx.fireChannelRead(parse);
        }
    }

    private Map<String, String> parse(FullHttpRequest request) throws Exception {
        Map<String, String> params = new HashMap();
        HttpMethod method = request.method();
        uriParse(request, params);
        if (HttpMethod.GET != method) {
            bodyParse(request, params);
        }
        return params;
    }

    private void uriParse(FullHttpRequest request, Map<String, String> params) {
        QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
        decoder.parameters().entrySet().forEach(entry -> {
            params.put(entry.getKey(), entry.getValue().get(0));
        });
    }

    private void bodyParse(FullHttpRequest request, Map<String, String> params) throws IOException {
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(request);
        decoder.offer(request);
        List<InterfaceHttpData> parmList = decoder.getBodyHttpDatas();
        for (InterfaceHttpData param : parmList) {
            Attribute data = (Attribute) param;
            params.put(data.getName(), data.getValue());
        }
    }
}
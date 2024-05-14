package com.dahuaboke.redisx.web;

import com.dahuaboke.redisx.handler.DirtyDataHandler;
import com.dahuaboke.redisx.web.handler.WebHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2024/5/13 10:32
 * auth: dahua
 * desc:
 */
public class WebServer {

    private static final Logger logger = LoggerFactory.getLogger(WebServer.class);

    private WebContext webContext;
    private EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    private EventLoopGroup workerGroup = new NioEventLoopGroup(1);

    public WebServer(WebContext webContext) {
        this.webContext = webContext;
    }

    /**
     * 启动方法
     */
    public void start() {
        String host = webContext.getHost();
        int port = webContext.getPort();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(512 * 1024));
                        pipeline.addLast(new WebHandler(webContext));
                        pipeline.addLast(new DirtyDataHandler());
                    }
                });
        if (host != null) {
            serverBootstrap.bind(host, port);
        } else {
            serverBootstrap.bind(port);
        }
        logger.info("Publish server at [{}:{}]", host, port);
    }

    /**
     * 销毁方法
     */
    public void destroy() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

}

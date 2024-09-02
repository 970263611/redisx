package com.dahuaboke.redisx.console;

import com.dahuaboke.redisx.console.handler.ConsoleHandler;
import com.dahuaboke.redisx.console.handler.MonitorHandler;
import com.dahuaboke.redisx.console.handler.ReplyHandler;
import com.dahuaboke.redisx.console.handler.SearchHandler;
import com.dahuaboke.redisx.handler.DirtyDataHandler;
import com.dahuaboke.redisx.handler.PrintHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * 2024/5/15 9:49
 * auth: dahua
 * desc:
 */
public class ConsoleServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleServer.class);

    private ConsoleContext consoleContext;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    public ConsoleServer(ConsoleContext consoleContext, Executor bossExecutor, Executor workerExecutor) {
        this.consoleContext = consoleContext;
        bossGroup = new NioEventLoopGroup(1, bossExecutor);
        workerGroup = new NioEventLoopGroup(1, workerExecutor);
    }

    /**
     * 启动方法
     */
    public void start() {
        String host = consoleContext.getHost();
        int port = consoleContext.getPort();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer() {
                        @Override
                        protected void initChannel(Channel channel) throws Exception {
                            ChannelPipeline pipeline = channel.pipeline();
                            pipeline.addLast(new PrintHandler());
                            pipeline.addLast(new HttpServerCodec());
                            pipeline.addLast(new HttpObjectAggregator(512 * 1024));
                            pipeline.addLast(new ConsoleHandler());
                            pipeline.addLast(new SearchHandler(consoleContext));
                            pipeline.addLast(new MonitorHandler(consoleContext));
                            pipeline.addLast(new ReplyHandler());
                            pipeline.addLast(new DirtyDataHandler());
                        }
                    });
            if (host != null) {
                channel = serverBootstrap.bind(host, port).sync().channel();
            } else {
                channel = serverBootstrap.bind(port).sync().channel();
            }
            logger.info("Publish console server at [{}:{}]", host, port);
        } catch (Exception e) {
            logger.error("Publish at {{}:{}] exception", host, port, e);
        }
    }

    /**
     * 销毁方法
     */
    public void destroy() {
        consoleContext.setClose(true);
        if (channel != null && channel.isActive()) {
            channel.close();
            try {
                channel.closeFuture().addListener((ChannelFutureListener) channelFuture -> {
                    if (channelFuture.isSuccess()) {
                        bossGroup.shutdownGracefully();
                        workerGroup.shutdownGracefully();
                        logger.info("Close [From] [{}:{}]", consoleContext.getHost(), consoleContext.getPort());
                    } else {
                        logger.error("Close [From] error", channelFuture.cause());
                    }
                }).sync();
            } catch (InterruptedException e) {
                logger.error("Close [From] error", e);
            }
        }
    }
}

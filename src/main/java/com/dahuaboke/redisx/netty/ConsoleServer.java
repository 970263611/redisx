package com.dahuaboke.redisx.netty;

import com.dahuaboke.redisx.netty.handler.HttpRequestParamParser;
import com.dahuaboke.redisx.netty.handler.WebReceiveHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.net.InetSocketAddress;

/**
 * author: dahua
 * date: 2024/2/27 15:42
 */
public class ConsoleServer extends Thread {

    private String host;
    private int port;
    private String remoteHost;
    private int remotePort;

    public ConsoleServer(String host, int port, String remoteHost, int remotePort) {
        this.host = host;
        this.port = port;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
    }

    @Override
    public void run() {
        InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workGroup = new NioEventLoopGroup(1);
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer() {
                    @Override
                    protected void initChannel(Channel channel) {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast(new HttpServerCodec());
                        pipeline.addLast(new HttpObjectAggregator(512 * 1024));
                        pipeline.addLast(new HttpRequestParamParser());
                        pipeline.addLast(new WebReceiveHandler(remoteHost,remotePort));
                    }
                })
                .localAddress(socketAddress)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true);
        try {
            ChannelFuture future = bootstrap.bind(socketAddress).sync();
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
        } finally {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }
}

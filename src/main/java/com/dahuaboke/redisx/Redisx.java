package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        //目标地址集合
        List<InetSocketAddress> forwards = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 17101));
            add(new InetSocketAddress("192.168.20.100", 17102));
            add(new InetSocketAddress("192.168.20.100", 17103));
            add(new InetSocketAddress("192.168.20.100", 17104));
            add(new InetSocketAddress("192.168.20.100", 17105));
            add(new InetSocketAddress("192.168.20.100", 17106));
        }};
        //源地址集合
        List<InetSocketAddress> slaves = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 17001));
            add(new InetSocketAddress("192.168.20.100", 17002));
            add(new InetSocketAddress("192.168.20.100", 17003));
            add(new InetSocketAddress("192.168.20.100", 17004));
            add(new InetSocketAddress("192.168.20.100", 17005));
            add(new InetSocketAddress("192.168.20.100", 17006));
        }};
        InetSocketAddress console = new InetSocketAddress("127.0.0.1", 9090);
        int consoleTimeout = 5000;

        //源 目标 是否集群模式
        boolean forwarderIsCluster = true;
        boolean masterIsCluster = true;

        Context context = new Context(forwarderIsCluster, masterIsCluster);
        context.start(forwards, slaves, console, consoleTimeout);
    }
}
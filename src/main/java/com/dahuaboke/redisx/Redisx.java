package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        //目标地址集合
        List<InetSocketAddress> forwards = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 16101));
            add(new InetSocketAddress("192.168.20.100", 16102));
            add(new InetSocketAddress("192.168.20.100", 16103));
            add(new InetSocketAddress("192.168.20.100", 16104));
            add(new InetSocketAddress("192.168.20.100", 16105));
            add(new InetSocketAddress("192.168.20.100", 16106));
        }};
        //源地址集合
        List<InetSocketAddress> slaves = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 16001));
            add(new InetSocketAddress("192.168.20.100", 16002));
            add(new InetSocketAddress("192.168.20.100", 16003));
            add(new InetSocketAddress("192.168.20.100", 16004));
            add(new InetSocketAddress("192.168.20.100", 16005));
            add(new InetSocketAddress("192.168.20.100", 16006));
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
package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        //目标地址集合
        List<InetSocketAddress> forwards = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 7001));
        }};
        //源地址集合
        List<InetSocketAddress> slaves = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 7002));
        }};
        InetSocketAddress console = new InetSocketAddress("127.0.0.1", 9090);
        int consoleTimeout = 5000;

        //源 目标 是否集群模式
        boolean forwarderIsCluster = false;
        boolean masterIsCluster = false;

        Context context = new Context(forwarderIsCluster, masterIsCluster);
        context.start(forwards, slaves, console, consoleTimeout);
    }
}
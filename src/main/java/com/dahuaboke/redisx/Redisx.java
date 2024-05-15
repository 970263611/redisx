package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        List<InetSocketAddress> forwards = new ArrayList() {{
            add(new InetSocketAddress("127.0.0.1", 6380));
        }};
        List<InetSocketAddress> slaves = new ArrayList() {{
            add(new InetSocketAddress("127.0.0.1", 6379));
        }};
        InetSocketAddress console = new InetSocketAddress("127.0.0.1", 9090);
        int consoleTimeout = 5000;

        boolean forwarderIsCluster = false;
        boolean masterIsCluster = false;

        Context context = new Context(forwarderIsCluster, masterIsCluster);
        context.start(forwards, slaves, console, consoleTimeout);
    }
}
package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        List<InetSocketAddress> tos = new ArrayList() {{
            add(new InetSocketAddress("127.0.0.1", 6382));
        }};
        List<InetSocketAddress> froms = new ArrayList() {{
            add(new InetSocketAddress("127.0.0.1", 6381));
        }};
        InetSocketAddress console = new InetSocketAddress("127.0.0.1", 9090);
        int consoleTimeout = 5000;

        boolean toIsCluster = false;
        boolean fromIsCluster = false;

        Context context = new Context(toIsCluster, fromIsCluster);
        context.start(tos, froms, console, consoleTimeout);
    }
}
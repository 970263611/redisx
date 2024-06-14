package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        List<InetSocketAddress> tos = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 17101));
            add(new InetSocketAddress("192.168.20.100", 17102));
            add(new InetSocketAddress("192.168.20.100", 17106));
//            add(new InetSocketAddress("192.168.20.100", 17104));
//            add(new InetSocketAddress("192.168.20.100", 17105));
//            add(new InetSocketAddress("192.168.20.100", 17106));
        }};
        List<InetSocketAddress> froms = new ArrayList() {{
            add(new InetSocketAddress("192.168.20.100", 17001));
            add(new InetSocketAddress("192.168.20.100", 17002));
            add(new InetSocketAddress("192.168.20.100", 17003));
//            add(new InetSocketAddress("192.168.20.100", 17004));
//            add(new InetSocketAddress("192.168.20.100", 17005));
//            add(new InetSocketAddress("192.168.20.100", 17006));
        }};
        InetSocketAddress console = new InetSocketAddress("127.0.0.1", 9090);
        int consoleTimeout = 5000;

        boolean startConsole = true;
        boolean toIsCluster = false;
        boolean fromIsCluster = false;

        Controller controller = new Controller(toIsCluster, fromIsCluster);
        controller.start(tos, froms, startConsole, console, consoleTimeout);
    }
}
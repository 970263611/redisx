package com.dahuaboke.redisx;


import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static String hostname = "192.168.1.26";

    public static void main(String[] args) {
        List<InetSocketAddress> tos = new ArrayList() {{
            add(new InetSocketAddress(hostname, 17101));
            add(new InetSocketAddress(hostname, 17102));
            add(new InetSocketAddress(hostname, 17103));
//            add(new InetSocketAddress("192.168.20.100", 17104));
//            add(new InetSocketAddress("192.168.20.100", 17105));
//            add(new InetSocketAddress("192.168.20.100", 17106));
        }};
        List<InetSocketAddress> froms = new ArrayList() {{
            add(new InetSocketAddress(hostname, 17001));
            add(new InetSocketAddress(hostname, 17002));
            add(new InetSocketAddress(hostname, 17003));
//            add(new InetSocketAddress("192.168.20.100", 17004));
//            add(new InetSocketAddress("192.168.20.100", 17005));
//            add(new InetSocketAddress("192.168.20.100", 17006));
        }};
        InetSocketAddress console = new InetSocketAddress(hostname, 9090);
        int consoleTimeout = 5000;

        boolean startConsole = false;
        boolean toIsCluster = true;
        boolean fromIsCluster = true;

        Controller controller = new Controller(toIsCluster, fromIsCluster);
        controller.start(tos, froms, startConsole, console, consoleTimeout);
    }
}
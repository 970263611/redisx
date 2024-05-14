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
        InetSocketAddress webAddress = new InetSocketAddress("127.0.0.1", 9090);
        Context context = new Context();
        context.start(forwards, slaves, webAddress);
    }
}
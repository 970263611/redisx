package com.dahuaboke.redisx;


import com.dahuaboke.redisx.netty.ConsoleServer;

public class Application {

    public static void main(String[] args) {
        ConsoleServer consoleServer = new ConsoleServer("127.0.0.1", 8080, "127.0.0.1", 6379);
        consoleServer.start();
    }
}
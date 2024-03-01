package com.dahuaboke.redisx;


import com.dahuaboke.redisx.netty.ConsoleServer;
import com.dahuaboke.redisx.sync.SyncClient;

public class Application {

    public static void main(String[] args) {
        ConsoleServer consoleServer = new ConsoleServer("127.0.0.1", 8080, "127.0.0.1", 6379);
        consoleServer.start();
        SyncClient syncClient = new SyncClient("127.0.0.1", 6379);
        syncClient.start();
    }
}
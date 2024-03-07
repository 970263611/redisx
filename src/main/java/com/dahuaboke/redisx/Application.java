package com.dahuaboke.redisx;


import com.dahuaboke.redisx.netty.ConsoleServer;
import com.dahuaboke.redisx.sync.SyncClient;

public class Application {

    public static void main(String[] args) throws InterruptedException {
        ConsoleServer consoleServer = new ConsoleServer("127.0.0.1", 8080, "127.0.0.1", 6379);
        consoleServer.start();
        Thread.sleep(10000);
        SyncClient syncClient = new SyncClient("127.0.0.1", 6379);
        syncClient.start();
    }
}
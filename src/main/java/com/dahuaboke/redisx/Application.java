package com.dahuaboke.redisx;


import com.dahuaboke.redisx.slave.SlaveClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        new SlaveClient("127.0.0.1", 6379).start();
    }
}
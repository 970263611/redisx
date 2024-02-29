package com.dahuaboke.redisx;


import com.dahuaboke.redisx.netty.RedisClient;
import com.dahuaboke.redisx.netty.handler.RedisMessageHandler;
import com.dahuaboke.redisx.netty.handler.WebReceiveHandler;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * author: dahua
 * date: 2024/2/27 16:25
 */
public class Context {

    private final LinkedBlockingDeque<String> SEND_TOPIC = new LinkedBlockingDeque();
    private final LinkedBlockingDeque<String> RECEIVE_TOPIC = new LinkedBlockingDeque();
    private Publisher publisher;
    private Subscriber subscriber;
    private volatile boolean isClose = false;
    private RedisClient redisClient;

    public void register(RedisMessageHandler redisMessageHandler) {
        publisher = new Publisher(redisMessageHandler);
        publisher.setName("publisher-" + System.currentTimeMillis());
        publisher.start();
    }

    public void register(WebReceiveHandler webReceiveHandler) {
        subscriber = new Subscriber(webReceiveHandler);
        subscriber.setName("subscriber-" + System.currentTimeMillis());
        subscriber.start();
    }

    public void subscribe(String command) {
        subscriber.subscribe(command);
    }

    public void callBack(String callBack) {
        publisher.receive(callBack);
    }

    public void destroy() {
        redisClient.destroy();
        this.isClose = true;
    }

    private class Publisher extends Thread {

        private RedisMessageHandler redisMessageHandler;

        public Publisher(RedisMessageHandler redisMessageHandler) {
            this.redisMessageHandler = redisMessageHandler;
        }

        @Override
        public void run() {
            while (!isClose) {
                String command = SEND_TOPIC.poll();
                if (command != null) {
                    publish(command);
                }
            }
        }

        public void receive(String callBack) {
            if (!RECEIVE_TOPIC.offer(callBack)) {
                receive(callBack);
            }
        }

        private void publish(String command) {
            redisMessageHandler.sendCommand(command);
        }
    }

    private class Subscriber extends Thread {

        private WebReceiveHandler webReceiveHandler;

        public Subscriber(WebReceiveHandler webReceiveHandler) {
            this.webReceiveHandler = webReceiveHandler;
        }

        private void subscribe(String command) {
            if (!SEND_TOPIC.offer(command)) {
                subscribe(command);
            }
        }

        @Override
        public void run() {
            while (!isClose) {
                try {
                    String callBack = RECEIVE_TOPIC.take();
                    if (callBack != null) {
                        webReceiveHandler.receive(callBack);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void setRedisClient(RedisClient redisClient) {
        this.redisClient = redisClient;
    }
}

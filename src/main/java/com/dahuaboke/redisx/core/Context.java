package com.dahuaboke.redisx.core;


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

    public void register(Sender sender) {
        publisher = new Publisher(sender);
        publisher.setName("publisher-" + System.currentTimeMillis());
        publisher.start();
    }

    public void register(Receiver receiver) {
        subscriber = new Subscriber(receiver);
        subscriber.setName("subscriber-" + System.currentTimeMillis());
        subscriber.start();
    }

    public void send(String command) {
        subscriber.send(command);
    }

    public void callBack(String callBack) {
        publisher.receive(callBack);
    }

    public void destroy() {
        this.isClose = true;
    }

    private class Publisher extends Thread {

        private Sender sender;

        public Publisher(Sender sender) {
            this.sender = sender;
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
            sender.send(command);
        }
    }

    private class Subscriber extends Thread {

        private Receiver receiver;

        public Subscriber(Receiver receiver) {
            this.receiver = receiver;
        }

        private void send(String command) {
            if (!SEND_TOPIC.offer(command)) {
                send(command);
            }
        }

        @Override
        public void run() {
            while (!isClose) {
                try {
                    String callBack = RECEIVE_TOPIC.take();
                    if (callBack != null) {
                        receiver.receive(callBack);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}

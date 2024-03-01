package com.dahuaboke.redisx.sync;

import com.dahuaboke.redisx.core.Context;
import com.dahuaboke.redisx.core.Receiver;

/**
 * author: dahua
 * date: 2024/3/1 9:45
 */
public class SyncReceiver implements Receiver {

    private Context context;
    private volatile State state = State.ONE;

    public SyncReceiver(Context context) {
        this.context = context;
    }

    @Override
    public void receive(String callBack) {
        System.out.println(callBack);
        switch (state) {
            case ONE -> {
                context.send("PING");
                state = State.TWO;
            }
            case TWO -> {
                context.send("REPLCONF listening-port 8080");
                state = State.THREE;
            }
            case THREE -> {
                context.send("REPLCONF ip-address 127.0.0.1");
                state = State.FOUR;
            }
            case FOUR -> {
                context.send("REPLCONF capa eof");
                state = State.FIVE;
            }
            case FIVE -> {
                context.send("PSYNC ? -1");
                state = State.END;
            }
        }
    }

    public enum State {
        ONE,
        TWO,
        THREE,
        FOUR,
        FIVE,
        END;
    }
}

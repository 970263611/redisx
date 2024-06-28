package dahuaboke.redisx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class RedissonUtilTest {

    //配置单点地址，或者集群服务器中任一地址,哨兵模式下需配置哨兵节点的ip端口
    private String toAddress = "redis://192.168.20.11:27101";

    private String fromAddress = "redis://192.168.20.11:27001";

    //类型
    private ServerType serverType = ServerType.SENTINEL;

    private RedissonClient toClient;

    private RedissonClient fromClient;

    enum ServerType{
        SINGLE,
        CLUSTER,
        SENTINEL;
    }

    @Before
    public void init() {
        Config toConfig = new Config();
        toConfig.setCodec(new StringCodec());
        Config fromConfig = new Config();
        fromConfig.setCodec(new StringCodec());
        if (ServerType.CLUSTER == serverType) {
            toConfig.useClusterServers().addNodeAddress(toAddress);
            fromConfig.useClusterServers().addNodeAddress(fromAddress);
        } else if(ServerType.SINGLE == serverType) {
            toConfig.useSingleServer().setAddress(toAddress);
            fromConfig.useSingleServer().setAddress(fromAddress);
        } else{
            toConfig.useSentinelServers().addSentinelAddress(toAddress).setCheckSentinelsList(false).setMasterName("mymaster");
            fromConfig.useSentinelServers().addSentinelAddress(fromAddress).setCheckSentinelsList(false).setMasterName("mymaster");
        }
        try {
            this.toClient = Redisson.create(toConfig);
        } catch (Exception e) {
        }
        try {
            this.fromClient = Redisson.create(fromConfig);
        } catch (Exception e) {
        }
    }

    @Test
    public void flushdb() {
        RKeys key1 = toClient.getKeys();
        key1.flushdb();
        RKeys key2 = fromClient.getKeys();
        key2.flushdb();
        keycount();
    }


    @Test
    public void testaaa(){
        RBucket<Object> aaaa = toClient.getBucket("fdfdfd");
        aaaa.set("dddfffd");
    }


    @Test
    public void keycount() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        AtomicLong last = new AtomicLong();
        AtomicLong max = new AtomicLong();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            SimpleDateFormat format = new SimpleDateFormat("mm:ss");
            RKeys key1 = toClient.getKeys();
            RKeys key2 = fromClient.getKeys();
            long to = key1.count();
            long from = key2.count();
            long c = to - last.get();
            System.out.println(from + "," + to + "," + (to - last.get())+","+max.get() );
//            System.out.println(format.format(new Date()) + "-" + c);
            last.set(to);
            max.set(Math.max(max.get(),c));
        },0,1, TimeUnit.SECONDS);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test1() {
        RKeys key1 = toClient.getKeys();
        Iterable<String> keys = key1.getKeys();
        Iterator<String> iterator = keys.iterator();
        //186613
        //186627
        int i = 0;
        while (iterator.hasNext()) {
            System.out.println(++i);
            fromClient.getBucket(iterator.next()).delete();
        }
        System.out.println("end");
    }

    @After
    public void destory() {
        try {
            if (toClient != null) {
                toClient.shutdown();
            }
        } catch (Exception e) {
        }
        try {
            if (fromClient != null) {
                fromClient.shutdown();
            }
        } catch (Exception e) {
        }
    }

}

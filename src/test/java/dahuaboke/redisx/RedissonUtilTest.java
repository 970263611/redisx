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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 每秒打印redis数据库数据总数，及相关增量参数
 */
public class RedissonUtilTest {

    //*********** 配置项 始 ***********//
    //配置单点地址，或者集群服务器中任一地址,哨兵模式下需配置哨兵节点的ip端口
    private String toAddress = "redis://xxx.xxx.xxx.xxx:port";

    private String fromAddress = "redis://xxx.xxx.xxx.xxx:port";

    //类型
    private ServerType serverType = ServerType.CLUSTER;
    //*********** 配置项 终 ***********//


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
    public void keycount() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        AtomicLong toLast = new AtomicLong();
        AtomicLong fromLast = new AtomicLong();
        AtomicLong toMax = new AtomicLong();
        AtomicLong fromMax = new AtomicLong();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        AtomicBoolean first = new AtomicBoolean();
        first.set(true);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(format.format(new Date())).append("] ");
            RKeys key1 = toClient.getKeys();
            RKeys key2 = fromClient.getKeys();
            long to = key1.count();
            long from = key2.count();
            long toC =  to - toLast.get();
            long formC =  from - fromLast.get();
            if(first.get()){
                toC = 0;
                formC = 0;
                first.set(false);
            }
            sb.append("-from:").append(cover(from + "",9)).append(", ");
            sb.append("-to:").append(cover(to + "",9)).append(", ");
            sb.append("-fromTps=").append(cover(formC + "",7)).append(", ");
            sb.append("-toTps=").append(cover(toC + "",7)).append(", ");
            sb.append("-fromMaxTps=").append(cover(fromMax.get() + "",7)).append(", ");
            sb.append("-toMaxTps=").append(cover(toMax.get() + "",7));
            System.out.println(sb.toString());
            toLast.set(to);
            fromLast.set(from);
            toMax.set(Math.max(toMax.get(),toC));
            fromMax.set(Math.max(fromMax.get(),formC));
        },0,1, TimeUnit.SECONDS);
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String cover(String s,int len){
        if(s == null){
            s = "";
        }
        StringBuilder sb = new StringBuilder(s);
        while((len - sb.length()) > 0){
            sb.insert(0," ");
        }
        return sb.toString();
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

package dahuaboke.redisx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;


import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class YaceTest {

    //配置单点地址，或者集群服务器中任一地址
    private String address = "redis://192.168.20.100:17001";

    //是否集群
    private boolean isCluster = true;

    //并发数
    private int threadCount = 20;

    //测试时间，秒
    private int second = 3600;

    //是否定时清理
    private boolean flushFlag = false;

    //清理周期，单位 秒
    private int flushSecond = 10;

    //是否要保证生成的key唯一
    private boolean onlyKey = true;

    private RedissonClient redisson;

    private Map<String,Long> countMap;

    private SecureRandom random = SecureRandom.getInstance("SHA1PRNG");

    private Set<Thread> threadSet = new HashSet<>();

    SnowflakeIdWorker idWorker = new SnowflakeIdWorker(0, 0);

    public YaceTest() throws NoSuchAlgorithmException {
    }

    @Before
    public void init(){
        Config config = new Config();
        config.setCodec(new StringCodec());
        config.setThreads(threadCount + 1);
        if(isCluster) {
            config.useClusterServers().addNodeAddress(address);
        }else{
            config.useSingleServer().setAddress(address);
        }
        this.redisson = Redisson.create(config);
        threadCount = threadCount < 1 ? 1 : threadCount;
        threadCount = threadCount > 100 ? 100 : threadCount;
        second = second < 1 ? 1 : second;
        countMap = new HashMap<String,Long>(){{
            put("num",1l);
            put("lastCount",0l);
            put("maxTps",0l);
        }};
        threadSet.add(new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                    addAll();
                }
            }
        }));
        if(flushFlag){
            flushSecond = flushSecond < 1 ? 1 : flushSecond;
            threadSet.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    while(true){
                        try {
                            Thread.sleep(flushSecond * 1000);
                        } catch (InterruptedException e) {
                            return;
                        }
                        RKeys rKeys = redisson.getKeys();
                        rKeys.flushdb();
                        System.out.println("进行了 数据清理操作");
                    }
                }
            }));
        }
        threadSet.forEach(t -> {
            t.setDaemon(true);
            t.start();
        });
    }

    @Test
    public void addData() throws InterruptedException {
        long endTime = System.currentTimeMillis() + second * 1000;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        for (int i = 0;i < threadCount;i++) {
            Thread t = new Thread(new Runnable() {
                long c = 0;
                @Override
                public void run() {
                    try {
                        while (true) {
                            redisson.getBucket(onlyKey ? String.valueOf(idWorker.nextId()) : getStr()).set(getStr());
                            countMap.put(Thread.currentThread().getName(), ++c);
                            if (System.currentTimeMillis() > endTime) {
                                countDownLatch.countDown();
                                return;
                            }
                        }
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            t.setDaemon(true);
            t.start();
        }
        countDownLatch.await();
        System.out.println(countMap);
    }

    @After
    public void destory(){
        threadSet.forEach(t -> {
            t.interrupt();
        });
        addAll();
        redisson.shutdown();
    }

    private void addAll(){
        long count = 0;
        for(Map.Entry<String,Long> entry : countMap.entrySet()){
            if(!"lastCount".equals(entry.getKey()) && !"num".equals(entry.getKey()) && !"maxTps".equals(entry.getKey())){
                count += entry.getValue();
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(countMap.get("num")).append("  ");
        sb.append("执行次数=").append(count);
        sb.append(",").append("tps=").append(count - countMap.get("lastCount"));
        sb.append(",").append("峰值tps=").append(countMap.get("maxTps"));
        System.out.println(sb.toString());
        countMap.put("maxTps",Math.max(countMap.get("maxTps"),count - countMap.get("lastCount")));
        countMap.put("num",countMap.get("num") + 1);
        countMap.put("lastCount",count);

    }

    /**
     * 生成长度1~30的随机字符串
     * @return
     * @throws NoSuchAlgorithmException
     */
    private String getStr() throws NoSuchAlgorithmException {
        return UUID.randomUUID().toString().replace("-","").substring(5,(random.nextInt(25) + 6));
    }

}

package dahuaboke.redisx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class YaceTest {

    private RedissonClient redisson;

    //并发数
    private int threadCount = 10;

    //测试时间，秒
    private int second = 2;

    //每个线程完成总次数
    private Map<String,Integer> countMap = new HashMap<>();

    private SecureRandom random = SecureRandom.getInstance("SHA1PRNG");

    public YaceTest() throws NoSuchAlgorithmException {
    }

    @Before
    public void init(){
        Config config = new Config();
        config.setCodec(new StringCodec());
        config.setThreads(threadCount * 2);
        config.useClusterServers()
                //此处配置集群中任意1个节点的地址
                .addNodeAddress("redis://192.168.20.100:16001");
        this.redisson = Redisson.create(config);
        threadCount = threadCount < 1 ? 1 : threadCount;
        threadCount = threadCount > 100 ? 100 : threadCount;
        second = second < 1 ? 1 : second;
    }

    @Test
    public void addData() throws InterruptedException {
        long endTime = System.currentTimeMillis() + second * 1000;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        Thread printCount = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    if(Thread.currentThread().isInterrupted()){
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                    addAll();
                }
            }
        });
        printCount.start();
        for (int i = 0;i < threadCount;i++) {
            new Thread(new Runnable() {
                int c = 0;
                @Override
                public void run() {
                    try {
                        while (true) {
                            redisson.getBucket(getStr()).set(getStr());
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
            }).start();
        }
        countDownLatch.await();
        printCount.interrupt();
        addAll();
    }

    @After
    public void destory(){
        redisson.shutdown();
    }

    private void addAll(){
        int count = 0;
        for(Map.Entry<String,Integer> entry : countMap.entrySet()){
            count += entry.getValue();
        }
        System.out.println("完成总次数 : " + count);
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

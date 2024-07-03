package dahuaboke.redisx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class StressTestingUtilTest {

    //配置单点地址，或者集群服务器中任一地址,哨兵模式下需配置哨兵节点的ip端口
    private String address = "redis://192.168.56.128:16001";

    //是否集群
    private ServerType serverType = ServerType.CLUSTER;

    //并发数
    private int threadCount = 20;

    //测试时间，秒
    private int second = 310;

    //是否定时清理
    private boolean flushFlag = false;

    //清理周期，单位 秒
    private int flushSecond = 10;

    //是否要保证生成的key唯一
    private boolean onlyKey = true;

    private RedissonClient redisson;

    private Map<String, Long> countMap;

    private SecureRandom random = SecureRandom.getInstance("SHA1PRNG");

    private Set<Thread> threadSet = new HashSet<>();

    SnowflakeIdWorker idWorker = new SnowflakeIdWorker(0, 0);

    enum ServerType {
        SINGLE,
        CLUSTER,
        SENTINEL;
    }

    public StressTestingUtilTest() throws NoSuchAlgorithmException {
    }

    @Before
    public void init() {
        Config config = new Config();
        config.setCodec(new StringCodec());
        config.setThreads(threadCount + 1);
        if (ServerType.CLUSTER == serverType) {
            config.useClusterServers().addNodeAddress(address);
        } else if (ServerType.SINGLE == serverType) {
            config.useSingleServer().setAddress(address);
        } else {
            config.useSentinelServers().addSentinelAddress(address).setCheckSentinelsList(false).setMasterName("mymaster");
        }
        this.redisson = Redisson.create(config);
        threadCount = threadCount < 1 ? 1 : threadCount;
        threadCount = threadCount > 100 ? 100 : threadCount;
        second = second < 1 ? 1 : second;
        countMap = new HashMap<String, Long>() {{
            put("num", 1l);
            put("lastCount", 0l);
            put("maxTps", 0l);
        }};
        threadSet.add(new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        return;
                    }
                    addAll();
                }
            }
        }));
        if (flushFlag) {
            flushSecond = flushSecond < 1 ? 1 : flushSecond;
            threadSet.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
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
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new Runnable() {
                long c = 0;

                @Override
                public void run() {
                    try {
                        while (true) {
                            //redisson.getBucket(onlyKey ? String.valueOf(idWorker.nextId()) : getStr()).set(getStr());
                            //redisson.getBucket(onlyKey ? String.valueOf(idWorker.nextId()) : getStr()).set("dimple");
                            random();
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

    public void random() throws NoSuchAlgorithmException {
        Random random1 = new Random();
        int i1 = random1.nextInt(5);
        switch (i1) {
            case 1:
                creatMap();
            case 2:
                creatList();
            case 3:
                creatSet();
            case 4:
                creatZSet();
            default:
                creatString();
        }

    }

    public void creatString() throws NoSuchAlgorithmException {
        redisson.getBucket(onlyKey ? String.valueOf(idWorker.nextId()) : getStr())
                .set(getStr());

    }

    public void creatMap() throws NoSuchAlgorithmException {
        RMap<Object, Object> map = redisson.getMap(onlyKey ? String.valueOf(idWorker.nextId()) : getStr());
        map.put("k1  k<>?:k1", "v1 <>??_+{“：》：” v1");
        map.put("k2  k<>?:k2", "v2 <>??_+{“：》：” v2");

    }

    public void creatList() throws NoSuchAlgorithmException {
        RList<Object> list = redisson.getList(onlyKey ? String.valueOf(idWorker.nextId()) : getStr());
        list.add("v3^&*^&&*()(+_)(6&*()(%^&*( ^{}|:>?v3");
        list.add("v3^&*^&&*()(+_)(6&*()(%^&*( ^{}|:>?v4");
    }

    public void creatSet() throws NoSuchAlgorithmException {
        RSet<Object> set = redisson.getSet(onlyKey ? String.valueOf(idWorker.nextId()) : getStr());
        set.add("v4 !@#$%^&*(~~！@#￥%……&*（{}|）——+：“《》？!@#$%^& &*(^&*Uv4");
        set.add("v4 <>??_+{“：》%^*&JNJj j：” v4");
    }

    public void creatZSet() throws NoSuchAlgorithmException {
        RScoredSortedSet<Object> scoredSortedSet = redisson.getScoredSortedSet(onlyKey ? String.valueOf(idWorker.nextId()) : getStr());
        scoredSortedSet.add(1.0,"v5   #$%^hjk&*……&*《》？：{}|“'' &*Uv5");
        scoredSortedSet.add(2.0,"v5 <>??_+{“：》%^*&JNJj j：” v5");
    }


    @After
    public void destory() {
        threadSet.forEach(t -> {
            t.interrupt();
        });
        addAll();
        redisson.shutdown();
    }

    private void addAll() {
        long count = 0;
        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
            if (!"lastCount".equals(entry.getKey()) && !"num".equals(entry.getKey()) && !"maxTps".equals(entry.getKey())) {
                count += entry.getValue();
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append(countMap.get("num")).append("-");
        sb.append(count);
        sb.append(",").append(countMap.get("maxTps"));
        sb.append(",").append(count - countMap.get("lastCount"));
        System.out.println(sb.toString());
        countMap.put("maxTps", Math.max(countMap.get("maxTps"), count - countMap.get("lastCount")));
        countMap.put("num", countMap.get("num") + 1);
        countMap.put("lastCount", count);
    }

//    private void addAll() {
//        long count = 0;
//        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
//            if (!"lastCount".equals(entry.getKey()) && !"num".equals(entry.getKey()) && !"maxTps".equals(entry.getKey())) {
//                count += entry.getValue();
//            }
//        }
//        SimpleDateFormat format = new SimpleDateFormat("mm:ss");
//        System.out.println(format.format(new Date()) + "-" + (count - countMap.get("lastCount")));
//        countMap.put("maxTps", Math.max(countMap.get("maxTps"), count - countMap.get("lastCount")));
//        countMap.put("num", countMap.get("num") + 1);
//        countMap.put("lastCount", count);
//    }

    /**
     * 生成长度1~30的随机字符串
     *
     * @return
     * @throws NoSuchAlgorithmException
     */
    private String getStr() throws NoSuchAlgorithmException {
        return UUID.randomUUID().toString().replace("-", "").substring(5, (random.nextInt(25) + 6));
    }


    class SnowflakeIdWorker {
        /**
         * 开始时间戳 (2015-01-01)
         */
        private final long twepoch = 1420041600000L;

        /**
         * 机器id所占的位数
         */
        private final long workerIdBits = 5L;

        /**
         * 数据标识id所占的位数
         */
        private final long datacenterIdBits = 5L;

        /**
         * 支持的最大机器id，结果是31 (这个移位算法可以很快的计算出几位二进制数所能表示的最大十进制数)
         */
        private final long maxWorkerId = -1L ^ (-1L << workerIdBits);

        /**
         * 支持的最大数据标识id，结果是31
         */
        private final long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);

        /**
         * 序列在id中占的位数
         */
        private final long sequenceBits = 12L;

        /**
         * 机器ID向左移12位
         */
        private final long workerIdShift = sequenceBits;

        /**
         * 数据标识id向左移17位(12+5)
         */
        private final long datacenterIdShift = sequenceBits + workerIdBits;

        /**
         * 时间戳向左移22位(5+5+12)
         */
        private final long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;

        /**
         * 生成序列的掩码，这里为4095 (0b111111111111=0xfff=4095)
         */
        private final long sequenceMask = -1L ^ (-1L << sequenceBits);

        /**
         * 工作机器ID(0~31)
         */
        private long workerId;

        /**
         * 数据中心ID(0~31)
         */
        private long datacenterId;

        /**
         * 毫秒内序列(0~4095)
         */
        private long sequence = 0L;

        /**
         * 上次生成ID的时间戳
         */
        private long lastTimestamp = -1L;

        //==============================Constructors=====================================

        /**
         * 构造函数
         *
         * @param workerId     工作ID (0~31)
         * @param datacenterId 数据中心ID (0~31)
         */
        public SnowflakeIdWorker(long workerId, long datacenterId) {
            if (workerId > maxWorkerId || workerId < 0) {
                throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
            }
            if (datacenterId > maxDatacenterId || datacenterId < 0) {
                throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
            }
            this.workerId = workerId;
            this.datacenterId = datacenterId;
        }

        private int i = 0;

        // ==============================Methods==========================================

        /**
         * 获得下一个ID (该方法是线程安全的)
         *
         * @return SnowflakeId
         */
        public synchronized long nextId() {
            long timestamp = timeGen();

            //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
            if (timestamp < lastTimestamp) {
                throw new RuntimeException(
                        String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
            }

            //如果是同一时间生成的，则进行毫秒内序列
            if (lastTimestamp == timestamp) {
                sequence = (sequence + 1) & sequenceMask;
                //毫秒内序列溢出
                if (sequence == 0) {
                    //阻塞到下一个毫秒,获得新的时间戳
                    timestamp = tilNextMillis(lastTimestamp);
                }
            }
            //时间戳改变，毫秒内序列重置
            else {
                sequence = 0L;
            }

            //上次生成ID的时间戳
            lastTimestamp = timestamp;

            //移位并通过或运算拼到一起组成64位的ID
            return ((timestamp - twepoch) << timestampLeftShift) //
                    | (datacenterId << datacenterIdShift) //
                    | (workerId << workerIdShift) //
                    | sequence;
        }

        /**
         * 阻塞到下一个毫秒，直到获得新的时间戳
         *
         * @param lastTimestamp 上次生成ID的时间戳
         * @return 当前时间戳
         */
        protected long tilNextMillis(long lastTimestamp) {
            long timestamp = timeGen();
            while (timestamp <= lastTimestamp) {
                timestamp = timeGen();
            }
            return timestamp;
        }

        /**
         * 返回以毫秒为单位的当前时间
         *
         * @return 当前时间(毫秒)
         */
        protected long timeGen() {
            return System.currentTimeMillis();
        }

    }

}

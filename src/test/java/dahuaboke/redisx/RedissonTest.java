package dahuaboke.redisx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

public class RedissonTest {

    //配置单点地址，或者集群服务器中任一地址
    private String forwardsAddress = "redis://192.168.20.100:16001";

    private String slavesAddress = "redis://192.168.20.100:16101";

    //是否集群
    private boolean isCluster = true;

    private RedissonClient forwardClient;

    private RedissonClient slavesClient;

    @Before
    public void init(){
        Config forwardConfig = new Config();
        forwardConfig.setCodec(new StringCodec());
        Config slavesConfig = new Config();
        slavesConfig.setCodec(new StringCodec());
        if(isCluster) {
            forwardConfig.useClusterServers().addNodeAddress(forwardsAddress);
            slavesConfig.useClusterServers().addNodeAddress(slavesAddress);
        }else{
            forwardConfig.useSingleServer().setAddress(forwardsAddress);
            slavesConfig.useSingleServer().setAddress(slavesAddress);
        }
        try {
            this.forwardClient = Redisson.create(forwardConfig);
        }catch (Exception e){}
        try {
            this.slavesClient = Redisson.create(slavesConfig);
        }catch (Exception e){}
    }

    @Test
    public void flushdb(){
        RKeys key1 = forwardClient.getKeys();
        key1.flushdb();
        RKeys key2 = slavesClient.getKeys();
        key2.flushdb();
    }

    @Test
    public void keycount(){
        RKeys key1 = forwardClient.getKeys();
        //keys.flushdb();
        System.out.println(key1.count());
        RKeys key2 = slavesClient.getKeys();
        //keys.flushdb();
        System.out.println(key2.count());
    }

    @After
    public void destory(){
        try {
            if(forwardClient != null){
                forwardClient.shutdown();
            }
        }catch (Exception e){}
        try {
            if(slavesClient != null){
                slavesClient.shutdown();
            }
        }catch (Exception e){}
    }

}

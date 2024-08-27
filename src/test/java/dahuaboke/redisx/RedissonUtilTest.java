package dahuaboke.redisx;

import com.dahuaboke.redisx.Redisx;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.utils.CRC16;
import com.dahuaboke.redisx.common.utils.FieldOrmUtil;
import com.dahuaboke.redisx.common.utils.StringUtils;
import com.dahuaboke.redisx.common.utils.YamlUtil;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RedissonUtilTest {

    //*********** 配置项 始 ***********//
    //配置单点地址，或者集群服务器中任一地址,哨兵模式下需配置哨兵节点的ip端口
    private String toAddress = null;

    private String fromAddress = null;

    //哨兵必填
    private String masterName = null;

    private String password = null;

    //类型
    private Mode serverType = null;
    //*********** 配置项 终 ***********//

    private RedissonClient toClient;

    private RedissonClient fromClient;

    @Before
    public void init() {
        Redisx.Config yamlConfig = new Redisx.Config();
        FieldOrmUtil.MapToBean(YamlUtil.parseYamlParam(null), yamlConfig);
        if(serverType == null){
            serverType = yamlConfig.getFromMode();
        }
        if(StringUtils.isEmpty(toAddress)){
            InetSocketAddress inetSocketAddress = yamlConfig.getToAddresses().get(0);
            toAddress = "redis://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();
        }
        if(StringUtils.isEmpty(fromAddress)){
            InetSocketAddress inetSocketAddress = yamlConfig.getFromAddresses().get(0);
            fromAddress = "redis://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();
        }
        if(StringUtils.isEmpty(masterName)){
            masterName = yamlConfig.getFromMasterName();
        }
        if(StringUtils.isEmpty(password)){
            password = yamlConfig.getFromPassword();
        }
        Config toConfig = new Config();
        toConfig.setCodec(new StringCodec());
        Config fromConfig = new Config();
        fromConfig.setCodec(new StringCodec());
        if (Mode.CLUSTER == serverType) {
            toConfig.useClusterServers().addNodeAddress(toAddress).setPassword(password);
            fromConfig.useClusterServers().addNodeAddress(fromAddress).setPassword(password);
        } else if (Mode.SINGLE == serverType) {
            toConfig.useSingleServer().setAddress(toAddress).setPassword(password);
            fromConfig.useSingleServer().setAddress(fromAddress).setPassword(password);
        } else {
            toConfig.useSentinelServers().addSentinelAddress(toAddress).setCheckSentinelsList(false).setMasterName(masterName).setPassword(password).setSentinelPassword(password);
            fromConfig.useSentinelServers().addSentinelAddress(fromAddress).setCheckSentinelsList(false).setMasterName(masterName).setPassword(password).setSentinelPassword(password);
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
    public void compareKey(){
        RKeys keyTo = toClient.getKeys();
        RKeys keyFrom = fromClient.getKeys();
        Iterator<String> ito = keyTo.getKeys().iterator();
        Set<String> toSet = new HashSet<>();
        while(ito.hasNext()){
            toSet.add(ito.next());
        }
        Iterator<String> ifrom = keyFrom.getKeys().iterator();
        while(ifrom.hasNext()){
            String key = ifrom.next();
            if(!toSet.contains(key)){
                System.out.println(key);
            }
        }
    }


    @Test
    public void slotNum(){
        RKeys keyFrom = fromClient.getKeys();
        Iterator<String> itFrom = keyFrom.getKeys().iterator();
        while(itFrom.hasNext()){
            String key = itFrom.next();
            int slot = CRC16.crc16(key.getBytes()) % 16384;
            if(slot == 0){
                System.out.println(key);
            }
        }

    }

}

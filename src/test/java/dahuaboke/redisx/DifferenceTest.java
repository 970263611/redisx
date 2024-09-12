package dahuaboke.redisx;

import com.dahuaboke.redisx.Redisx;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.utils.FieldOrmUtil;
import com.dahuaboke.redisx.common.utils.StringUtils;
import com.dahuaboke.redisx.common.utils.YamlUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
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

public class DifferenceTest {
    //*********** 配置项 始 ***********//
    //配置单点地址，或者集群服务器中任一地址,哨兵模式下需配置哨兵节点的ip端口
    private String toAddress = null;

    private String fromAddress = null;

    //哨兵必填
    private String masterNameFrom = null;

    private String masterNameTo = null;

    private String password = null;

    //类型
    private Mode serverTypeFrom = null;

    private Mode serverTypeTo = null;

    private String loglevel = "warn";
    //*********** 配置项 终 ***********//


    private RedissonClient toClient;

    private RedissonClient fromClient;

    @Before
    public void init() {
        Configurator.setRootLevel(Level.getLevel(loglevel));
        Redisx.Config yamlConfig = new Redisx.Config();
        FieldOrmUtil.MapToBean(YamlUtil.parseYamlParam(null), yamlConfig);
        if(serverTypeFrom == null){
            serverTypeFrom = yamlConfig.getFromMode();
        }
        if(serverTypeTo == null){
            serverTypeTo = yamlConfig.getToMode();
        }
        if(StringUtils.isEmpty(toAddress)){
            InetSocketAddress inetSocketAddress = yamlConfig.getToAddresses().get(0);
            toAddress = "redis://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();
        }
        if(StringUtils.isEmpty(fromAddress)){
            InetSocketAddress inetSocketAddress = yamlConfig.getFromAddresses().get(0);
            fromAddress = "redis://" + inetSocketAddress.getHostString() + ":" + inetSocketAddress.getPort();
        }
        if(StringUtils.isEmpty(masterNameFrom)){
            masterNameFrom = yamlConfig.getFromMasterName();
        }
        if(StringUtils.isEmpty(masterNameTo)){
            masterNameTo = yamlConfig.getToMasterName();
        }
        if(StringUtils.isEmpty(password)){
            password = yamlConfig.getFromPassword();
        }
        Config toConfig = new Config();
        toConfig.setCodec(new StringCodec());
        Config fromConfig = new Config();
        fromConfig.setCodec(new StringCodec());
        if (Mode.CLUSTER == serverTypeFrom) {
            fromConfig.useClusterServers().addNodeAddress(fromAddress).setPassword(password);
        } else if (Mode.SINGLE == serverTypeFrom) {
            fromConfig.useSingleServer().setAddress(fromAddress).setPassword(password);
        } else {
            fromConfig.useSentinelServers().addSentinelAddress(fromAddress).setCheckSentinelsList(false).setMasterName(masterNameFrom).setPassword(password).setSentinelPassword(password);
        }
        if (Mode.CLUSTER == serverTypeTo) {
            toConfig.useClusterServers().addNodeAddress(toAddress).setPassword(password);
        } else if (Mode.SINGLE == serverTypeTo) {
            toConfig.useSingleServer().setAddress(toAddress).setPassword(password);
        } else {
            toConfig.useSentinelServers().addSentinelAddress(toAddress).setCheckSentinelsList(false).setMasterName(masterNameTo).setPassword(password).setSentinelPassword(password);
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
    public void bidui(){
        RKeys fromkeys = fromClient.getKeys();
        RKeys tokeys = toClient.getKeys();
        Iterator<String> fromiterator = fromkeys.getKeys().iterator();
        Set<String> fromKetSet = new HashSet<String>();
        while(fromiterator.hasNext()){
            fromKetSet.add(fromiterator.next());
        }
        long m = fromkeys.count() - tokeys.count() + 1;
        int n = 0,sf=0,st=0,s=0;
        int maps=0,sets=0,lists=0,zsets=0,strs=0;
        int mapc=0,setc=0,listc=0,zsetc=0;
        Iterator<String> toiterator = tokeys.getKeys().iterator();
        while(toiterator.hasNext()){
            s++;
            if(s /1000 * 1000 == s){
                System.out.println("已处理 " + s + "条");
            }
            String tkey = toiterator.next();
            if(fromKetSet.contains(tkey)){
                String type = tokeys.getType(tkey).toString();
                switch (type){
                    case "MAP":
                        maps++;
                        sf = fromClient.getMap(tkey).size();
                        st = toClient.getMap(tkey).size();
                        if(sf != st){
                            mapc++;
                            System.out.println("MAP值差异=" + mapc);
                        }
                        break;
                    case "SET":
                        sets++;
                        sf = fromClient.getSet(tkey).size();
                        st = toClient.getSet(tkey).size();
                        if(sf != st){
                            setc++;
                            System.out.println("SET值差异=" + setc);
                        }
                        break;
                    case "LIST":
                        lists++;
                        sf = fromClient.getList(tkey).size();
                        st = toClient.getList(tkey).size();
                        if(sf != st){
                            listc++;
                            System.out.println("LIST值差异=" + listc);
                        }
                        break;
                    case "ZSET":
                        zsets++;
                        sf = fromClient.getScoredSortedSet(tkey).size();
                        st = toClient.getScoredSortedSet(tkey).size();
                        if(sf != st){
                            zsetc++;
                            System.out.println("ZSET值差异=" + zsetc);
                        }
                        break;
                    default:
                        strs++;
                        sf = 0;
                        st = 0;
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("from总量=").append(fromkeys.count());
        sb.append(" ,to总量=").append(tokeys.count() - 1);
        sb.append(" ,key数量差=").append(m);
        sb.append(" ,value差异量=").append(zsetc + listc + setc + mapc);
        sb.append(" ,MAP总量=").append(maps).append(" ,MAPvalue差异量=").append(mapc);
        sb.append(" ,SET总量=").append(sets).append(" ,SETvalue差异量=").append(setc);
        sb.append(" ,LIST总量=").append(lists).append(" ,LISTvalue差异量=").append(lists);
        sb.append(" ,ZSET总量=").append(zsets).append(" ,ZSETvalue差异量=").append(zsetc);
        sb.append(" ,STRING总量=").append(strs);
        System.out.println(sb.toString());
    }

}

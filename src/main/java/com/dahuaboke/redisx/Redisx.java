package com.dahuaboke.redisx;


import org.yaml.snakeyaml.Yaml;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Redisx {

    public static String hostname = "192.168.14.26";

    public static void main(String[] args) {
        Map<String,Object> config = praseConfig();
        List<InetSocketAddress> tos = new ArrayList() {{
            add(new InetSocketAddress(hostname, 16101));
            add(new InetSocketAddress(hostname, 16102));
            add(new InetSocketAddress(hostname, 16103));
//            add(new InetSocketAddress("hostname", 16104));
//            add(new InetSocketAddress("hostname", 16105));
//            add(new InetSocketAddress("hostname", 16106));
        }};
        List<InetSocketAddress> froms = new ArrayList() {{
            add(new InetSocketAddress(hostname, 16001));
            add(new InetSocketAddress(hostname, 16002));
            add(new InetSocketAddress(hostname, 16003));
//            add(new InetSocketAddress("hostname", 16004));
//            add(new InetSocketAddress("hostname", 16005));
//            add(new InetSocketAddress("hostname", 16006));
        }};
        InetSocketAddress console = new InetSocketAddress("localhost", 9090);
        int consoleTimeout = 5000;

        boolean startConsole = false;
        boolean toIsCluster = true;
        boolean fromIsCluster = true;

        Controller controller = new Controller(toIsCluster, fromIsCluster);
        controller.start(tos, froms, startConsole, console, consoleTimeout);
    }

    private static Map<String,Object> configMap = new HashMap<>();

    private static Map<String,Object> praseConfig(){
        Yaml yaml = new Yaml();
        Map<String,Object> map = yaml.load(Redisx.class.getClassLoader().getResourceAsStream("redisx.yml"));
        Map<String,Object> config = new HashMap<>();
        praseConfig(null,map,config);
        return config;
    }

    private static void praseConfig(String startKey,Object obj,Map<String,Object> config){
        if(obj instanceof Map){
            Map<String,Object> map = (Map<String, Object>) obj;
            for(Map.Entry<String,Object> entry : map.entrySet()){
                if(startKey == null){
                    praseConfig(entry.getKey(),entry.getValue(),config);
                }else{
                    praseConfig(startKey + "." + entry.getKey(),entry.getValue(),config);
                }
            }
        }else{
            config.put(startKey,obj);
        }
    }

}
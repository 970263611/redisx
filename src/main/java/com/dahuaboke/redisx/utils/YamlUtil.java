package com.dahuaboke.redisx.utils;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Redisx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 2024/6/20 15:12
 * auth: dahua
 * desc:
 */
public class YamlUtil {

    private static final Logger logger = LoggerFactory.getLogger(YamlUtil.class);

    public static Redisx.Config parseYamlParam(String fileName) {
        try {
            Map<String, Object> paramMap = parseConfig(fileName);
            boolean fromIsCluster = paramMap.get("redisx.from.isCluster") == null ? false : (boolean) paramMap.get("redisx.from.isCluster");
            String fromPassword = (String) paramMap.get("redisx.from.password");
            List<String> fromAddressStrList = (List<String>) paramMap.get("redisx.from.address");
            if (fromAddressStrList == null || fromAddressStrList.isEmpty()) {
                throw new IllegalArgumentException("redisx.from.address");
            }
            List<InetSocketAddress> fromAddresses = new ArrayList();
            for (String address : fromAddressStrList) {
                String[] hostAndPortAry = address.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPortAry[0], Integer.parseInt(hostAndPortAry[1]));
                fromAddresses.add(inetSocketAddress);
            }
            boolean toIsCluster = paramMap.get("redisx.to.isCluster") == null ? false : (boolean) paramMap.get("redisx.to.isCluster");
            String toPassword = (String) paramMap.get("redisx.to.password");
            List<String> toAddressStrList = (List<String>) paramMap.get("redisx.to.address");
            if (toAddressStrList == null || toAddressStrList.isEmpty()) {
                throw new IllegalArgumentException("redisx.to.address");
            }
            List<InetSocketAddress> toAddresses = new ArrayList();
            for (String address : toAddressStrList) {
                String[] hostAndPortAry = address.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPortAry[0], Integer.parseInt(hostAndPortAry[1]));
                toAddresses.add(inetSocketAddress);
            }
            boolean consoleEnable = paramMap.get("redisx.console.enable") == null ? false : (boolean) paramMap.get("redisx.console.enable");
            int consolePort = paramMap.get("redisx.console.port") == null ? 18080 : (int) paramMap.get("redisx.console.port");
            int consoleTimeout = paramMap.get("redisx.console.timeout") == null ? 5000 : (int) paramMap.get("redisx.console.timeout");
            boolean immediate = paramMap.get("redisx.immediate.enable") == null ? false : (boolean) paramMap.get("redisx.immediate.enable");
            int immediateResendTimes = paramMap.get("redisx.immediate.resendTimes") == null ? 0 : (int) paramMap.get("redisx.immediate.resendTimes");
            boolean alwaysFullSync = paramMap.get("redisx.alwaysFullSync") == null ? false : (boolean) paramMap.get("redisx.alwaysFullSync");
            String redisVersion = (String) paramMap.get("redisx.from.redis.version");
            if (redisVersion == null) {
                throw new IllegalArgumentException("redisx.from.redis.version");
            }
            String switchFlag = paramMap.get("redisx.switchFlag") == null ? Constant.SWITCH_FLAG : (String) paramMap.get("redisx.switchFlag");
            return new Redisx.Config(fromIsCluster, fromPassword, fromAddresses, toIsCluster, toPassword, toAddresses, consoleEnable, consolePort, consoleTimeout, immediate, alwaysFullSync, immediateResendTimes, redisVersion, switchFlag);
        } catch (Exception e) {
            logger.error("Config param error", e);
            System.exit(0);
        }
        return null;
    }

    private static Map<String, Object> parseConfig(String fileName) {
        Yaml yaml = new Yaml();
        Map<String, Object> map;
        if (fileName == null) {
            map = yaml.load(Redisx.class.getClassLoader().getResourceAsStream(Constant.CONFIG_FILE_NAME));
        } else {
            try {
                map = yaml.load(new FileInputStream(fileName));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        Map<String, Object> config = new HashMap();
        parseConfig(null, map, config);
        return config;
    }

    private static void parseConfig(String startKey, Object obj, Map<String, Object> config) {
        if (obj instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) obj;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (startKey == null) {
                    parseConfig(entry.getKey(), entry.getValue(), config);
                } else {
                    parseConfig(startKey + "." + entry.getKey(), entry.getValue(), config);
                }
            }
        } else {
            config.put(startKey, obj);
        }
    }
}

package com.dahuaboke.redisx.utils;

import com.dahuaboke.redisx.Constant;
import com.dahuaboke.redisx.Redisx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

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

    public static Redisx.Config parseYamlParam() {
        try {
            Map<String, Object> paramMap = parseConfig();
            boolean fromIsCluster = (boolean) paramMap.get("redisx.from.isCluster");
            String fromPassword = (String) paramMap.get("redisx.from.password");
            List<String> fromAddressStrList = (List<String>) paramMap.get("redisx.from.address");
            List<InetSocketAddress> fromAddresses = new ArrayList();
            for (String address : fromAddressStrList) {
                String[] hostAndPortAry = address.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPortAry[0], Integer.parseInt(hostAndPortAry[1]));
                fromAddresses.add(inetSocketAddress);
            }
            boolean toIsCluster = (boolean) paramMap.get("redisx.to.isCluster");
            String toPassword = (String) paramMap.get("redisx.to.password");
            List<String> toAddressStrList = (List<String>) paramMap.get("redisx.to.address");
            List<InetSocketAddress> toAddresses = new ArrayList();
            for (String address : toAddressStrList) {
                String[] hostAndPortAry = address.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPortAry[0], Integer.parseInt(hostAndPortAry[1]));
                toAddresses.add(inetSocketAddress);
            }
            boolean consoleEnable = (boolean) paramMap.get("redisx.console.enable");
            int consolePort = (int) paramMap.get("redisx.console.port");
            int consoleTimeout = (int) paramMap.get("redisx.console.timeout");
            boolean idempotency = (boolean) paramMap.get("redisx.idempotency");
            return new Redisx.Config(fromIsCluster, fromPassword, fromAddresses, toIsCluster, toPassword, toAddresses,
                    consoleEnable, consolePort, consoleTimeout, idempotency);
        } catch (Exception e) {
            logger.error("Config param error", e);
            System.exit(0);
        }
        return null;
    }

    private static Map<String, Object> parseConfig() {
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(Redisx.class.getClassLoader().getResourceAsStream(Constant.CONFIG_FILE_NAME));
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

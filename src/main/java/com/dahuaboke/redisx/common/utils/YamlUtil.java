package com.dahuaboke.redisx.common.utils;

import com.dahuaboke.redisx.Redisx;
import com.dahuaboke.redisx.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 2024/6/20 15:12
 * auth: dahua
 * desc:
 */
public class YamlUtil {

    private static final Logger logger = LoggerFactory.getLogger(YamlUtil.class);

    /**
     * 解析yaml文件为 map
     *
     * @param args
     * @return
     */
    public static Map<String, Object> parseYamlParam(String[] args) {
        try {
            String configPath = args == null || args.length == 0 ? null : args[0];
            Map<String, Object> paramMap = parseConfig(configPath);
            Properties properties = System.getProperties();
            for (String str : properties.stringPropertyNames()) {
                String val = properties.getProperty(str);
                if (StringUtils.isNotEmpty(str) && StringUtils.isNotEmpty(val)) {
                    paramMap.put(str, val);
                }
            }
            decryptMap(paramMap);
            return paramMap;
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
            map = yaml.load(Redisx.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILE_NAME));
        } else {
            try {
                map = yaml.load(new FileInputStream(fileName));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        Map<String, Object> config = new HashMap();
        decryptMap(config);
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

    /**
     * enc解密
     *
     * @param map
     */
    private static void decryptMap(Map<String, Object> map) {
        String password = (String) map.get("jasypt.encryptor.password");
        String algorithm = (String) map.get("jasypt.encryptor.algorithm");
        String ivGeneratorClassName = (String) map.get("jasypt.encryptor.ivGeneratorClassName");
        if (password == null || password.length() == 0) {
            return;
        }
        if (StringUtils.isEmpty(algorithm)) {
            algorithm = Constants.JASYPT_ALGORITHM;
        }
        if (StringUtils.isEmpty(ivGeneratorClassName)) {
            ivGeneratorClassName = Constants.JASYPT_IVGENERATORCLASSNAME;
        }
        JasyptUtil jasyptUtil = StringUtils.isNotEmpty(algorithm) ? new JasyptUtil(password, algorithm, ivGeneratorClassName) : new JasyptUtil(password);
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof String) {
                String s = (String) entry.getValue();
                if (s.startsWith("ENC(") && s.startsWith(")")) {
                    s = s.substring(4, s.length() - 1);
                    map.put(entry.getKey(), jasyptUtil.decrypt(s));
                }
            }
        }
    }
}

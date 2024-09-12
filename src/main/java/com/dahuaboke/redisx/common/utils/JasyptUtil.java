package com.dahuaboke.redisx.common.utils;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.EnvironmentPBEConfig;

public class JasyptUtil {

    private StandardPBEStringEncryptor encryptor;

    public JasyptUtil(String salt) {
        this(salt, null, null);
    }

    /**
     * @param salt                 盐
     * @param algorithm            加密算法
     * @param ivGeneratorClassName
     */
    public JasyptUtil(String salt, String algorithm, String ivGeneratorClassName) {
        if (salt == null || salt.length() == 0) {
            throw new RuntimeException("salt is null");
        }
        if (algorithm == null || algorithm.length() == 0) {
            algorithm = "PBEWithMD5AndDES";
        }
        if (ivGeneratorClassName == null || ivGeneratorClassName.length() == 0) {
            ivGeneratorClassName = "org.jasypt.iv.NoIvGenerator";
        }
        this.encryptor = new StandardPBEStringEncryptor();
        EnvironmentPBEConfig config = new EnvironmentPBEConfig();
        config.setAlgorithm(algorithm);
        config.setIvGeneratorClassName(ivGeneratorClassName);
        config.setPassword(salt);
        this.encryptor.setConfig(config);
    }

    /**
     * enc加密
     *
     * @param pwd 要加密的密文
     * @return
     */
    public String encrypt(String pwd) {
        return encryptor.encrypt(pwd);
    }

    /**
     * enc解密
     *
     * @param pwd 要解密的密文
     * @return
     */
    public String decrypt(String pwd) {
        return encryptor.decrypt(pwd);
    }

}

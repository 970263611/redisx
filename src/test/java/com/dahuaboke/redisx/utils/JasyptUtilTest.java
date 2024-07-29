package com.dahuaboke.redisx.utils;

import org.junit.Test;

public class JasyptUtilTest {

    private String algorithm = "PBEWithMD5AndDES";

    private String ivGeneratorClassName = "org.jasypt.iv.NoIvGenerator";

    //加密的盐
    private String salt = "KU1aBcAit9x";

    @Test
    public void encryptTest(){
        //明文密码
        String pwd = "123456";
        JasyptUtil jasyptUtil = new JasyptUtil(salt,algorithm,ivGeneratorClassName);
        System.out.println(jasyptUtil.encrypt(pwd));
    }

    @Test
    public void decryptTest(){
        //密文密码
        String pwd = "0mx8HSmdHaFlaJ2StK09PQ==";
        JasyptUtil jasyptUtil = new JasyptUtil(salt,algorithm,ivGeneratorClassName);
        System.out.println(jasyptUtil.decrypt(pwd));
    }

}

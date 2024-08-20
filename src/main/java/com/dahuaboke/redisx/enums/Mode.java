package com.dahuaboke.redisx.enums;

import com.dahuaboke.redisx.utils.StringUtils;

public enum Mode {

    SINGLE,
    CLUSTER,
    SENTINEL;

    /**
     * 根据字符串获取mode类型，不区分字符串大小写
     * @param name
     * @return
     */
    public static Mode getModeByString(String name){
        if(StringUtils.isNotEmpty(name)){
            for(Mode mode : values()){
                if(mode.name().equals(name.toUpperCase())){
                    return mode;
                }
            }
        }
        return null;
    }

}

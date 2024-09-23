package com.dahuaboke.redisx.common.enums;

import com.dahuaboke.redisx.common.utils.StringUtils;

public enum FilterType {

    NEEDFUL,
    NEEDLESS;

    /**
     * 根据字符串获取FilterType类型，不区分字符串大小写
     *
     * @param name
     * @return
     */
    public static FilterType getFilterTypeByString(String name) {
        if (StringUtils.isNotEmpty(name)) {
            for (FilterType filterType : values()) {
                if (filterType.name().equals(name.toUpperCase())) {
                    return filterType;
                }
            }
        }
        return null;
    }
}

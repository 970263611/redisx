package com.dahuaboke.redisx.slave.rdb;

import java.io.Serializable;

/**
 * @Desc: maxmemory-policy
 * @Author：cdl
 * @Date：2024/5/20 12:00
 */
public enum EvictType implements Serializable {

    /**
     * maxmemory-policy : volatile-lru, allkeys-lru. unit : second
     */
    LRU,

    /**
     * maxmemory-policy : volatile-lfu, allkeys-lfu.
     */
    LFU,

    /**
     * maxmemory-policy : noeviction, volatile-random, allkeys-random, volatile-ttl.
     */
    NONE
}

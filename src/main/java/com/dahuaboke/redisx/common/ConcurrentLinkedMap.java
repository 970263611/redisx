package com.dahuaboke.redisx.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConcurrentLinkedMap<K, V> extends HashMap<K, V> {

    private List<K> keys = new LinkedList<>();
    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public K getFirstKey() {
        lock.readLock().lock();
        try {
            if (keys.isEmpty()) {
                return null;
            }
            return keys.get(0);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void putIndex(K key) {
        lock.writeLock().lock();
        try {
            keys.add(key);
            super.put(key, null);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void putValue(K key, V value) {
        lock.writeLock().lock();
        try {
            super.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeKey(Object key) {
        lock.writeLock().lock();
        try {
            keys.remove(key);
            super.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }
}

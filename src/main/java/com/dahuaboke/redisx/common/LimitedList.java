package com.dahuaboke.redisx.common;

import java.util.LinkedList;

public class LimitedList<L> extends LinkedList<L> {
    private final int limitSize;

    public LimitedList(int limitSize) {
        this.limitSize = limitSize;
    }

    @Override
    public boolean add(L l) {
        super.add(l);
        while (size() > limitSize) {
            remove();
        }
        return true;
    }

    public boolean checkNeedUpgradeMaster() {
        L previous = get(0);
        for (int i = 1; i < size(); i++) {
            L v = get(i);
            if (!v.equals(previous)) {
                return false;
            } else {
                previous = v;
            }
        }
        return true;
    }

    public Long getLastSecondCount() {
        int size = size();
        if (size == 0) {
            return 0L;
        } else if (size == 1) {
            return (Long) get(size - 1);
        } else {
            return ((Long) get(size() - 1)) - ((Long) get(size() - 2));
        }
    }

    public Long getTps() {
        int size = size();
        if (size == 0) {
            return 0L;
        } else if (size == 1) {
            return (Long) get(size - 1);
        } else {
            return (((Long) get(size() - 1)) - ((Long) get(0))) / size;
        }
    }
}
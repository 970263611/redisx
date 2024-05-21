package com.dahuaboke.redisx.slave.zhh.zset;

import java.util.Arrays;
import java.util.Objects;

/**
 * @Desc: ZSet的有序性是通过以下方式实现的：
 *           1.分数关联：每个添加到ZSet中的元素都会与一个分数相关联。这个分数决定了元素在ZSet中的位置。
 *           2.排序：当元素被添加到ZSet中时，Redis会根据元素的分数将它们插入到适当的位置，以保持有序性。
 *            如果多个元素具有相同的分数，那么它们会按照字典序进行排序。
 *           3.自动更新：当ZSet中元素的分数被修改时，Redis会自动重新调整元素的位置，以保持有序性。
 * @Author：zhh
 * @Date：2024/5/20 17:34
 */
public class ZSetEntry {
    private static final long serialVersionUID = 1L;
    private byte[] element;
    private double score;

    public ZSetEntry() {
    }

    public ZSetEntry(byte[] element, double score) {
        this.element = element;
        this.score = score;
    }

    public byte[] getElement() {
        return element;
    }

    public double getScore() {
        return score;
    }

    public void setElement(byte[] element) {
        this.element = element;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "[" + element.toString() + ", " + score + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZSetEntry zSetEntry = (ZSetEntry) o;
        return Double.compare(zSetEntry.score, score) == 0 &&
                Arrays.equals(element, zSetEntry.element);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(score);
        result = 31 * result + Arrays.hashCode(element);
        return result;
    }
}

package com.dahuaboke.redisx.from.rdb.stream;

import java.io.Serializable;
import java.util.*;

/**
 * @Desc: Redis Stream(流,实现原理与kafka,RocketMq类似)有一个消息链表
 * 将所有加入的消息都串起来，每个消息都有一个唯一的 ID(自增) 和对应的内容。
 *
 * <p>
 * - ID: <millisecondsTime>-<sequenceNumber> 流中元素的key，在我们首次使用 xadd 指令追加消息时自动创建。
 * 毫秒时间部分实际上是生成流 ID 的本地 Redis 节点中的本地时间,序列号用于在同一毫秒内创建的条目。
 * - value: json格式的字符串。
 * - Consumer Group:    消费组，使用 XGROUP CREATE 命令创建，一个消费组有多个消费者(Consumer)。
 * - last_delivered_id: 游标，每个消费组会有个游标 last_delivered_id,任意一个消费者读取了消息都会使游标 last_delivered_id 往前移动。
 * - Consumer:  消费者
 * - pending_ids:   消费者(Consumer)的状态变量，作用是维护消费者的未确认的 id。pending_ids: 记录了当前已经被客户端读取的消息，但是还没有 ack (Acknowledge character：确认字符）。
 * <p>
 * @Author：zhh
 * @Date：2024/5/20 15:18
 */
public class Stream implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * 流最后一个元素Id
     */
    private ID lastId;
    /**
     * 版本 > redis 7.0
     * 流第一个元素Id
     */
    private ID firstId;
    /**
     * 版本 > redis 7.0
     * 被删除元素的最大Id
     */
    private ID maxDeletedEntryId;
    /**
     * 版本 > redis 7.0
     * 流中add的元素个数,并不代表length,因为不仅能add还能del,length代表最终结果,add是添加次数
     */
    private Long entriesAdded;
    /**
     * 流中元素
     */
    private NavigableMap<ID, Entry> entries;
    /**
     * 流元素个数
     */
    private long length;
    /**
     * 消费者组
     */
    private List<Group> groups;

    public Stream() {

    }

    public Stream(ID lastId, NavigableMap<ID, Entry> entries, long length, List<Group> groups) {
        this.lastId = lastId;
        this.entries = entries;
        this.length = length;
        this.groups = groups;
    }

    public Stream(ID lastId, NavigableMap<ID, Entry> entries, long length, List<Group> groups, ID firstId, ID maxDeletedEntryId, Long entriesAdded) {
        this(lastId, entries, length, groups);
        this.firstId = firstId;
        this.maxDeletedEntryId = maxDeletedEntryId;
        this.entriesAdded = entriesAdded;
    }

    public ID getLastId() {
        return lastId;
    }

    public void setLastId(ID lastId) {
        this.lastId = lastId;
    }

    public ID getFirstId() {
        return firstId;
    }

    public void setFirstId(ID firstId) {
        this.firstId = firstId;
    }

    public ID getMaxDeletedEntryId() {
        return maxDeletedEntryId;
    }

    public void setMaxDeletedEntryId(ID maxDeletedEntryId) {
        this.maxDeletedEntryId = maxDeletedEntryId;
    }

    public Long getEntriesAdded() {
        return entriesAdded;
    }

    public void setEntriesAdded(Long entriesAdded) {
        this.entriesAdded = entriesAdded;
    }

    public NavigableMap<ID, Entry> getEntries() {
        return entries;
    }

    public void setEntries(NavigableMap<ID, Entry> entries) {
        this.entries = entries;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    @Override
    public String toString() {
        String r = "Stream{" + "lastId=" + lastId + ", length=" + length;
        r += ", firstId=" + firstId + ", maxDeletedEntryId=" + maxDeletedEntryId + ", entriesAdded=" + entriesAdded;
        if (groups != null && !groups.isEmpty()) r += ", groups=" + groups;
        if (entries != null && !entries.isEmpty()) r += ", entries=" + entries.size();
        return r + '}';
    }

    public static class Entry implements Serializable {
        private static final long serialVersionUID = 1L;
        private ID id;
        private boolean deleted;
        private Map<byte[], byte[]> fields;

        public Entry() {

        }

        public Entry(ID id, boolean deleted, Map<byte[], byte[]> fields) {
            this.id = id;
            this.deleted = deleted;
            this.fields = fields;
        }

        public ID getId() {
            return id;
        }

        public void setId(ID id) {
            this.id = id;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public void setDeleted(boolean deleted) {
            this.deleted = deleted;
        }

        public Map<byte[], byte[]> getFields() {
            return fields;
        }

        public void setFields(Map<byte[], byte[]> fields) {
            this.fields = fields;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "id=" + id +
                    ", deleted=" + deleted +
                    ", fields=" + fields +
                    '}';
        }
    }

    public static class Group implements Serializable {
        private static final long serialVersionUID = 1L;
        private byte[] name;
        private ID lastId;
        private Long entriesRead;
        private NavigableMap<ID, Nack> pendingEntries;
        private List<Consumer> consumers;

        public Group() {

        }

        public Group(byte[] name, ID lastId, NavigableMap<ID, Nack> pendingEntries, List<Consumer> consumers) {
            this.name = name;
            this.lastId = lastId;
            this.pendingEntries = pendingEntries;
            this.consumers = consumers;
        }

        public Group(byte[] name, ID lastId, NavigableMap<ID, Nack> pendingEntries, List<Consumer> consumers, Long entriesRead) {
            this(name, lastId, pendingEntries, consumers);
            this.entriesRead = entriesRead;
        }

        public byte[] getName() {
            return name;
        }

        public void setName(byte[] name) {
            this.name = name;
        }

        public ID getLastId() {
            return lastId;
        }

        public void setLastId(ID lastId) {
            this.lastId = lastId;
        }

        public Long getEntriesRead() {
            return entriesRead;
        }

        public void setEntriesRead(Long entriesRead) {
            this.entriesRead = entriesRead;
        }

        public NavigableMap<ID, Nack> getPendingEntries() {
            return pendingEntries;
        }

        public void setPendingEntries(NavigableMap<ID, Nack> pendingEntries) {
            this.pendingEntries = pendingEntries;
        }

        public List<Consumer> getConsumers() {
            return consumers;
        }

        public void setConsumers(List<Consumer> consumers) {
            this.consumers = consumers;
        }

        @Override
        public String toString() {
            String r = "Group{" + "name='" + new String(name) + '\'' + ", lastId=" + lastId + ", entriesRead=" + entriesRead;
            if (consumers != null && !consumers.isEmpty()) r += ", consumers=" + consumers;
            if (pendingEntries != null && !pendingEntries.isEmpty()) r += ", gpel=" + pendingEntries.size();
            return r + '}';
        }
    }

    public static class Consumer implements Serializable {
        private static final long serialVersionUID = 1L;
        private byte[] name;
        private long seenTime;
        private long activeTime = -1; /* since redis 7.2 */
        private NavigableMap<ID, Nack> pendingEntries;

        public Consumer() {

        }

        public Consumer(byte[] name, long seenTime, NavigableMap<ID, Nack> pendingEntries) {
            this.name = name;
            this.seenTime = seenTime;
            this.pendingEntries = pendingEntries;
        }

        public byte[] getName() {
            return name;
        }

        public void setName(byte[] name) {
            this.name = name;
        }

        public long getSeenTime() {
            return seenTime;
        }

        public void setSeenTime(long seenTime) {
            this.seenTime = seenTime;
        }

        public long getActiveTime() {
            return activeTime;
        }

        public void setActiveTime(long activeTime) {
            this.activeTime = activeTime;
        }

        public NavigableMap<ID, Nack> getPendingEntries() {
            return pendingEntries;
        }

        public void setPendingEntries(NavigableMap<ID, Nack> pendingEntries) {
            this.pendingEntries = pendingEntries;
        }

        @Override
        public String toString() {
            String r = "Consumer{" + "name='" + new String(name) + '\'' + ", seenTime=" + seenTime + ", activeTime=" + activeTime;
            if (pendingEntries != null && !pendingEntries.isEmpty()) r += ", cpel=" + pendingEntries.size();
            return r + '}';
        }
    }

    public static class Nack implements Serializable {
        private static final long serialVersionUID = 1L;
        private ID id;
        private Consumer consumer;
        private long deliveryTime;
        private long deliveryCount;

        public Nack() {

        }

        public Nack(ID id, Consumer consumer, long deliveryTime, long deliveryCount) {
            this.id = id;
            this.consumer = consumer;
            this.deliveryTime = deliveryTime;
            this.deliveryCount = deliveryCount;
        }

        public ID getId() {
            return id;
        }

        public void setId(ID id) {
            this.id = id;
        }

        public Consumer getConsumer() {
            return consumer;
        }

        public void setConsumer(Consumer consumer) {
            this.consumer = consumer;
        }

        public long getDeliveryTime() {
            return deliveryTime;
        }

        public void setDeliveryTime(long deliveryTime) {
            this.deliveryTime = deliveryTime;
        }

        public long getDeliveryCount() {
            return deliveryCount;
        }

        public void setDeliveryCount(long deliveryCount) {
            this.deliveryCount = deliveryCount;
        }

        @Override
        public String toString() {
            return "Nack{" +
                    "id=" + id +
                    ", consumer=" + consumer +
                    ", deliveryTime=" + deliveryTime +
                    ", deliveryCount=" + deliveryCount +
                    '}';
        }
    }

    public static class ID implements Serializable, Comparable<ID> {
        private static final long serialVersionUID = 1L;
        public static Comparator<ID> COMPARATOR = comparator();

        private long ms;
        private long seq;

        public ID() {

        }

        public ID(long ms, long seq) {
            this.ms = ms;
            this.seq = seq;
        }

        public long getMs() {
            return ms;
        }

        public void setMs(long ms) {
            this.ms = ms;
        }

        public long getSeq() {
            return seq;
        }

        public void setSeq(long seq) {
            this.seq = seq;
        }

        public ID delta(long ms, long seq) {
            return new ID(this.ms + ms, this.seq + seq);
        }

        @Override
        public String toString() {
            return ms + "-" + seq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ID id = (ID) o;
            return ms == id.ms && seq == id.seq;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ms, seq);
        }

        @Override
        public int compareTo(ID that) {
            int r = Long.compare(this.ms, that.ms);
            if (r == 0) return Long.compare(this.seq, that.seq);
            return r;
        }

        public static ID valueOf(String id) {
            int idx = id.indexOf('-');
            long ms = Long.parseLong(id.substring(0, idx));
            long seq = Long.parseLong(id.substring(idx + 1, id.length()));
            return new ID(ms, seq);
        }

        public static ID valueOf(String strMs, String strSeq) {
            long ms = Long.parseLong(strMs);
            long seq = Long.parseLong(strSeq);
            return new ID(ms, seq);
        }

        public static Comparator<ID> comparator() {
            return new Comparator<ID>() {
                @Override
                public int compare(ID o1, ID o2) {
                    return o1.compareTo(o2);
                }
            };
        }
    }
}

package com.zleying.model;

public class TopicPartitionOffset {
    String topic;
    int partition;
    long fromOffset;
    long untilOffset;

    public TopicPartitionOffset() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getFromOffset() {
        return fromOffset;
    }

    public void setFromOffset(long fromOffset) {
        this.fromOffset = fromOffset;
    }

    public long getUntilOffset() {
        return untilOffset;
    }

    public void setUntilOffset(long untilOffset) {
        this.untilOffset = untilOffset;
    }
}

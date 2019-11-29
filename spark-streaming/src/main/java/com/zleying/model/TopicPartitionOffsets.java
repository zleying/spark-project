package com.zleying.model;

import java.util.List;

public class TopicPartitionOffsets {


    List<TopicPartitionOffset> topicPartitonOffsets;

    public TopicPartitionOffsets(List<TopicPartitionOffset> topicPartitonOffsets) {
        this.topicPartitonOffsets = topicPartitonOffsets;
    }

    public List<TopicPartitionOffset> getTopicPartitonOffsets() {
        return topicPartitonOffsets;
    }

    public void setTopicPartitonOffsets(List<TopicPartitionOffset> topicPartitonOffsets) {
        this.topicPartitonOffsets = topicPartitonOffsets;
    }
}

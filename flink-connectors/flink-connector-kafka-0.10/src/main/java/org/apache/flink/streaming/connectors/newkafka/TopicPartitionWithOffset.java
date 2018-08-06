package org.apache.flink.streaming.connectors.newkafka;

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;

class TopicPartitionWithOffset {

	private final TopicPartition topicPartition;
	private final long offset;

	public TopicPartitionWithOffset(TopicPartition topicPartition, long offset) {
		this.topicPartition = topicPartition;
		this.offset = offset;
	}

	public TopicPartition getTopicPartition() {
		return topicPartition;
	}

	public long getOffset() {
		return offset;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TopicPartitionWithOffset that = (TopicPartitionWithOffset) o;
		return offset == that.offset &&
				Objects.equals(topicPartition, that.topicPartition);
	}

	@Override
	public int hashCode() {
		return Objects.hash(topicPartition, offset);
	}
}

package org.apache.flink.streaming.connectors.newkafka;

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;

class TopicPartitionWithOffsetSerializer extends TypeSerializerSingleton<TopicPartitionWithOffset> {

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TopicPartitionWithOffset createInstance() {
		return null;
	}

	@Override
	public TopicPartitionWithOffset copy(TopicPartitionWithOffset from) {
		return from;
	}

	@Override
	public TopicPartitionWithOffset copy(
			TopicPartitionWithOffset from, TopicPartitionWithOffset reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(
			TopicPartitionWithOffset record, DataOutputView target) throws IOException {
		target.writeUTF(record.getTopicPartition().topic());
		target.writeInt(record.getTopicPartition().partition());
		target.writeLong(record.getOffset());
	}

	@Override
	public TopicPartitionWithOffset deserialize(DataInputView source) throws IOException {
		String topic = source.readUTF();
		int partition = source.readInt();
		long offset = source.readLong();
		return new TopicPartitionWithOffset(new TopicPartition(topic, partition), offset);
	}

	@Override
	public TopicPartitionWithOffset deserialize(
			TopicPartitionWithOffset reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(
			DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof TopicPartitionWithOffsetSerializer;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TopicPartitionWithOffsetSerializer;
	}

	@Override
	public int hashCode() {
		return TopicPartitionWithOffsetSerializer.class.hashCode();
	}
}

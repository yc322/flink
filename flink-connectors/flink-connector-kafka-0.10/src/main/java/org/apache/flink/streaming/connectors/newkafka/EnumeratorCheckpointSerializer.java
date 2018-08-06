package org.apache.flink.streaming.connectors.newkafka;

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

class EnumeratorCheckpointSerializer extends TypeSerializerSingleton<Set<TopicPartition>> {

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Set<TopicPartition> createInstance() {
		return null;
	}

	@Override
	public Set<TopicPartition> copy(Set<TopicPartition> from) {
		return from;
	}

	@Override
	public Set<TopicPartition> copy(
			Set<TopicPartition> from, Set<TopicPartition> reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(
			Set<TopicPartition> record, DataOutputView target) throws IOException {
		target.writeInt(record.size());
		for (TopicPartition partition : record) {
			target.writeUTF(partition.topic());
			target.writeInt(partition.partition());
		}
	}

	@Override
	public Set<TopicPartition> deserialize(DataInputView source) throws IOException {
		int size = source.readInt();
		Set<TopicPartition> result = new HashSet<>(size);
		for (int i = 0; i < size; i++) {
			String topic = source.readUTF();
			int partition = source.readInt();
			result.add(new TopicPartition(topic, partition));
		}
		return result;
	}

	@Override
	public Set<TopicPartition> deserialize(
			Set<TopicPartition> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(
			DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof EnumeratorCheckpointSerializer;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof EnumeratorCheckpointSerializer;
	}

	@Override
	public int hashCode() {
		return EnumeratorCheckpointSerializer.class.hashCode();
	}
}

package org.apache.flink.streaming.connectors.newkafka;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.sources.Source;
import org.apache.flink.streaming.api.sources.SplitEnumerator;
import org.apache.flink.streaming.api.sources.SplitReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * Naive new Kafka Consumer. Note how all the actual logic is in {@link KafkaFixedTopicsEnumerator}
 * and {@link KafkaPartitionReader} and the latter one does not care about topic discovery.
 */
public class KafkaConsumer<K, V> implements Source<Tuple2<K, V>, TopicPartitionWithOffset, Set<TopicPartition>> {

	/** User-supplied properties for Kafka. **/
	private final Properties properties;

	private final KafkaTopicsDescriptor topicsDescriptor;

	private final Class<? extends Deserializer<K>> keyDeserializer;
	private final Class<? extends Deserializer<V>> valueDeserializer;

	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;

	public KafkaConsumer(
			Properties properties,
			KafkaTopicsDescriptor topicsDescriptor,
			Class<? extends Deserializer<K>> keyDeserializer,
			Class<? extends Deserializer<V>> valueDeserializer,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer) {
		this.properties = properties;
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
		this.topicsDescriptor = topicsDescriptor;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		this.properties.setProperty(
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				keyDeserializer.getName());
		this.properties.setProperty(
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				valueDeserializer.getName());
	}

	@Override
	public TypeSerializer<TopicPartitionWithOffset> getSplitSerializer() {
		return new TopicPartitionWithOffsetSerializer();
	}

	@Override
	public TypeSerializer<Tuple2<K, V>> getElementSerializer() {
		return new TupleSerializer<Tuple2<K, V>>(
				(Class) Tuple2.class,
				new TypeSerializer[]{keySerializer, valueSerializer});
	}

	@Override
	public TypeSerializer<Set<TopicPartition>> getEnumeratorCheckpointSerializer() {
		return new EnumeratorCheckpointSerializer();
	}

	@Override
	public Set<TopicPartition> createInitialEnumeratorCheckpoint() {
		return Collections.emptySet();
	}

	@Override
	public SplitEnumerator<TopicPartitionWithOffset, Set<TopicPartition>> createSplitEnumerator(
			Set<TopicPartition> checkpoint) {
		return new KafkaFixedTopicsEnumerator(
				properties, topicsDescriptor.getFixedTopics(), checkpoint);
	}

	@Override
	public SplitReader<Tuple2<K, V>, TopicPartitionWithOffset> createSplitReader(
			final TopicPartitionWithOffset split) {
		return new KafkaPartitionReader<>(properties, split);
	}

}

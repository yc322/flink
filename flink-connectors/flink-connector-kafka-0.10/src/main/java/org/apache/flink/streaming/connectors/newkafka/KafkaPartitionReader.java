package org.apache.flink.streaming.connectors.newkafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.sources.SplitReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Note: we could also put the fetching logic into a different thread and use a handover as the
 * current Flink consumer does. This would fetch records asynchronously while this implementation
 * currently does is synchronously.
 */
class KafkaPartitionReader<K, V> implements SplitReader<Tuple2<K, V>, TopicPartitionWithOffset> {
	private final Properties kafkaProperties;

	private final TopicPartitionWithOffset split;

	private transient KafkaConsumer<K, V> kafkaConsumer;
	private transient Iterator<ConsumerRecord<K, V>> recordsIterator;
	private transient ConsumerRecord<K, V> currentRecord;

	public KafkaPartitionReader(
			Properties kafkaProperties,
			TopicPartitionWithOffset split) {
		this.kafkaProperties = kafkaProperties;
		this.split = split;
	}

	@Override
	public boolean start() throws IOException {
		kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
		kafkaConsumer.assign(Collections.singletonList(split.getTopicPartition()));
		ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(1000);
		recordsIterator = consumerRecords.iterator();
		if (recordsIterator.hasNext()) {
			currentRecord = recordsIterator.next();
		} else {
			currentRecord = null;
		}
		return currentRecord != null;
	}

	@Override
	public boolean advance() throws IOException {
		if (recordsIterator.hasNext()) {
			currentRecord = recordsIterator.next();
			return true;
		}

		ConsumerRecords<K, V> consumerRecords = kafkaConsumer.poll(1000);
		recordsIterator = consumerRecords.iterator();
		if (recordsIterator.hasNext()) {
			currentRecord = recordsIterator.next();
		} else {
			currentRecord = null;
		}
		return currentRecord != null;
	}

	@Override
	public Tuple2<K, V> getCurrent() throws NoSuchElementException {
		if (currentRecord != null) {
			return new Tuple2<>(currentRecord.key(), currentRecord.value());
		}
		throw new NoSuchElementException();
	}

	@Override
	public long getCurrentTimestamp() throws NoSuchElementException {
		return 0;
	}

	@Override
	public long getWatermark() {
		return 0;
	}

	@Override
	public TopicPartitionWithOffset checkpoint() {
		return split;
	}

	@Override
	public boolean isDone() throws IOException {
		return false;
	}

	@Override
	public void close() throws IOException {

	}
}

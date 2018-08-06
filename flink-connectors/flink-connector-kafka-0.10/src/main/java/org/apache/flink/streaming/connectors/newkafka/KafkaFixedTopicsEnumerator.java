/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.newkafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.sources.SplitEnumerator;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
class KafkaFixedTopicsEnumerator implements SplitEnumerator<TopicPartitionWithOffset, Set<TopicPartition>> {

	private final Properties kafkaProperties;

	private final List<String> topics;

	private Set<TopicPartition> alreadyDiscoveredPartitions;

	private transient KafkaConsumer<?, ?> kafkaConsumer;

	public KafkaFixedTopicsEnumerator(
			Properties kafkaProperties,
			List<String> subscribedTopics,
			Set<TopicPartition> alreadyDiscoveredPartitions) {
		this.kafkaProperties = checkNotNull(kafkaProperties);
		this.topics = checkNotNull(subscribedTopics);
		this.alreadyDiscoveredPartitions = new HashSet<>();
		this.alreadyDiscoveredPartitions.addAll(alreadyDiscoveredPartitions);
	}

	@Override
	public Iterable<TopicPartitionWithOffset> discoverNewSplits() {

		if (kafkaConsumer == null) {
			kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
		}

		// (1) get all possible newPartitions
		List<TopicPartition> newPartitions = new ArrayList<>();
		for (String topic : topics) {
			for (PartitionInfo partitionInfo : kafkaConsumer.partitionsFor(topic)) {
				newPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
			}
		}

		List<TopicPartitionWithOffset> result = new ArrayList<>();
		// (2) eliminate partition that are old newPartitions or should not be subscribed by this subtask
		for (TopicPartition partition : newPartitions) {
			if (alreadyDiscoveredPartitions.add(partition)) {
				result.add(new TopicPartitionWithOffset(partition, 0L));
			}
		}

		return result;
	}

	@Override
	public Set<TopicPartition> checkpoint() {
		return alreadyDiscoveredPartitions;
	}
}

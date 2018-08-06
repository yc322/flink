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

package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.newkafka.KafkaConsumer;
import org.apache.flink.streaming.connectors.newkafka.KafkaTopicsDescriptor;
import org.apache.flink.util.Collector;

import com.google.common.base.Stopwatch;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

/**
 * A simple example that shows how to read from and write to Kafka. This will read String messages
 * from the input topic, parse them into a POJO type {@link KafkaEvent}, group by some key, and finally
 * perform a rolling addition on each key for which the results are written back to another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition
 * watermarks directly in the Flink Kafka consumer. For demonstration purposes, it is assumed that
 * the String messages are of formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage:
 * 	--input-topic test-input --output-topic test-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 */
public class NewKafka010Example {

	public static void main(String[] args) throws Exception {
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 5) {
			System.out.println("Missing parameters!\n" +
					"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
					"--bootstrap.servers <kafka brokers> " +
					"--zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		env.setParallelism(1);

		Properties properties = parameterTool.getProperties();
		properties.setProperty("group.id", "group" + new Random().nextInt());

		KafkaConsumer<String, String> newKafkaConsumer = new KafkaConsumer<>(
				properties,
				new KafkaTopicsDescriptor(
						Collections.singletonList(parameterTool.getRequired("input-topic")), null),
				StringDeserializer.class,
				StringDeserializer.class,
				StringSerializer.INSTANCE,
				StringSerializer.INSTANCE);
		DataStream<String> inputStream = env
				.addSource(newKafkaConsumer)
				.map((i) -> i.f1);

//		FlinkKafkaConsumer010<String> oldKafkaConsumer = new FlinkKafkaConsumer010<>(
//				parameterTool.getRequired("input-topic"),
//				new SimpleStringSchema(),
//				properties);
//		DataStream<String> inputStream = env
//				.addSource(oldKafkaConsumer)
//				.map((i) -> i);

		inputStream.flatMap(new RichFlatMapFunction<String, String>() {
			Stopwatch stopwatch;
			long numElements;

			@Override
			public void open(Configuration parameters) throws Exception {
				stopwatch = new Stopwatch();
				stopwatch.start();
				numElements = 0L;
			}

			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
//				System.out.println("GOT: " + value);
				numElements++;

				if (numElements > 1_000_000) {
					long duration = stopwatch.elapsedMillis();
					double eps = (double) numElements / (double) duration * 1000.0d;
					System.out.println("EPS: " + eps);
					numElements = 0;
					stopwatch.reset();
					stopwatch.start();
				}
			}
		});



		env.execute("Kafka 0.10 Example");
	}
}

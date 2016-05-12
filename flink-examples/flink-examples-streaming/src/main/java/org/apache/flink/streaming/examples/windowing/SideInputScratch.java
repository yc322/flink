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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.sideinput.SingletonSideInput;

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 *
 * <p>
 * Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt; --window &lt;n&gt; --slide &lt;n&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>
 * This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>use basic windowing abstractions.
 * </ul>
 *
 */
public class SideInputScratch {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.disableOperatorChaining();

		DataStreamSource<String> source1 = env.fromElements("Hello", "There", "What", "up");
		DataStreamSource<Integer> sideSource1 = env.fromElements(1, 2, 3, 4, 5);
		DataStreamSource<String> sideSource2 = env.fromElements("A", "B", "C");

		SingletonSideInput<Integer, Object> sideInput1 = new SingletonSideInput<>(sideSource1.broadcast());
		SingletonSideInput<String, Object> sideInput2 = new SingletonSideInput<>(sideSource2.broadcast());

		source1
				.map(new RichMapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						System.out.println("SEEING MAIN INPUT: " + value);
						return value;
					}
				})
				.addSideInput(sideInput1)
				.addSideInput(sideInput2);
//				.print();


		env.execute("operator NG");
	}
}

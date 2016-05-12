/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

public class OneInputOperatorWrapper<IN, OUT> extends StreamOperatorNG<OUT> {
	private final OneInputStreamOperator<IN, OUT> operator;

	private final Input<IN> input = new Input<IN>() {
		@Override
		public void processElement(StreamRecord<IN> element) throws Exception {
			operator.processElement(element);
		}

		@Override
		public void processWatermark(Watermark watermark) throws Exception {
			operator.processWatermark(watermark);
		}
	};

	public OneInputOperatorWrapper(OneInputStreamOperator<IN, OUT> operator) {

		System.out.println("Creating wrapper for: " + operator);
		this.operator = operator;
		addInput(input);
	}

	public Input<IN> getInput() {
		return input;
	}

	@Override
	public void setup(OperatorStreamTask<?> containingTask,
			StreamConfig config,
			Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		operator.setup(null, config, output);
	}

	@Override
	public void open() throws Exception {
		super.open();
		operator.open();
	}

	@Override
	public void close() throws Exception {
		super.close();
		operator.close();
	}

	@Override
	public void dispose() {
		super.dispose();
		operator.dispose();
	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId,
			long timestamp) throws Exception {
		return super.snapshotOperatorState(checkpointId, timestamp);
	}

	@Override
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
		super.restoreState(state, recoveryTimestamp);
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);
	}
}

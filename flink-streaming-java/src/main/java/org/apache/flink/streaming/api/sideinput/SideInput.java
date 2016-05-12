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
package org.apache.flink.streaming.api.sideinput;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.UUID;

public abstract class SideInput<I, T> implements Serializable {

	private final UUID uuid = UUID.randomUUID();

	private transient DataStream<I> inputStream;

	public SideInput(DataStream<I> inputStream) {
		this.inputStream = inputStream;
	}

	public DataStream<I> getInputStream() {
		return inputStream;
	}

	public void setInputStream(DataStream<I> inputStream) {
		this.inputStream = inputStream;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SideInput sideInput = (SideInput) o;

		return uuid.equals(sideInput.uuid);

	}

	@Override
	public int hashCode() {
		return uuid.hashCode();
	}
}

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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import static java.util.Objects.requireNonNull;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 * 
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSource<T> extends DataStream<T> {

	boolean isParallel;

	public DataStreamSource(StreamExecutionEnvironment environment,
			TypeInformation<T> outTypeInfo, StreamSource<T, ?> operator,
			boolean isParallel, String sourceName) {
		super(environment, new SourceTransformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

		this.isParallel = isParallel;
		if (!isParallel) {
			setParallelism(1);
		}
	}

	public DataStreamSource<T> setParallelism(int parallelism) {
		if (parallelism > 1 && !isParallel) {
			throw new IllegalArgumentException("Source: " + transformation.getId() + " is not a parallel source");
		} else {
			transformation.setParallelism(parallelism);
			return this;
		}
	}

	/**
	 * Sets the name of the current data stream. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named operator.
	 */
	public DataStreamSource<T> name(String name){
		transformation.setName(name);
		return this;
	}

	// ------------------------------------------------------------------------
	//  Type hinting
	// ------------------------------------------------------------------------

	/**
	 * Adds a type information hint about the return type of this operator. This method
	 * can be used in cases where Flink cannot determine automatically what the produced
	 * type of a function is. That can be the case if the function uses generic type variables
	 * in the return type that cannot be inferred from the input type.
	 *
	 * <p>Classes can be used as type hints for non-generic types (classes without generic parameters),
	 * but not for generic types like for example Tuples. For those generic types, please
	 * use the {@link #returns(TypeHint)} method.
	 *
	 * @param typeClass The class of the returned data type.
	 * @return This operator with the type information corresponding to the given type class.
	 */
	public DataStreamSource<T> returns(Class<T> typeClass) {
		requireNonNull(typeClass, "type class must not be null.");

		try {
			return returns(TypeInformation.of(typeClass));
		}
		catch (InvalidTypesException e) {
			throw new InvalidTypesException("Cannot infer the type information from the class alone." +
					"This is most likely because the class represents a generic type. In that case," +
					"please use the 'returns(TypeHint)' method instead.");
		}
	}

	/**
	 * Adds a type information hint about the return type of this operator. This method
	 * can be used in cases where Flink cannot determine automatically what the produced
	 * type of a function is. That can be the case if the function uses generic type variables
	 * in the return type that cannot be inferred from the input type.
	 *
	 * <p>Use this method the following way:
	 * <pre>{@code
	 *     DataStream<Tuple2<String, Double>> result =
	 *         stream.flatMap(new FunctionWithNonInferrableReturnType())
	 *               .returns(new TypeHint<Tuple2<String, Double>>(){});
	 * }</pre>
	 *
	 * @param typeHint The type hint for the returned data type.
	 * @return This operator with the type information corresponding to the given type hint.
	 */
	public DataStreamSource<T> returns(TypeHint<T> typeHint) {
		requireNonNull(typeHint, "TypeHint must not be null");

		try {
			return returns(TypeInformation.of(typeHint));
		}
		catch (InvalidTypesException e) {
			throw new InvalidTypesException("Cannot infer the type information from the type hint. " +
					"Make sure that the TypeHint does not use any generic type variables.");
		}
	}

	/**
	 * Adds a type information hint about the return type of this operator. This method
	 * can be used in cases where Flink cannot determine automatically what the produced
	 * type of a function is. That can be the case if the function uses generic type variables
	 * in the return type that cannot be inferred from the input type.
	 *
	 * <p>In most cases, the methods {@link #returns(Class)} and {@link #returns(TypeHint)}
	 * are preferable.
	 *
	 * @param typeInfo type information as a return type hint
	 * @return This operator with a given return type hint.
	 */
	public DataStreamSource<T> returns(TypeInformation<T> typeInfo) {
		requireNonNull(typeInfo, "TypeInformation must not be null");

		transformation.setOutputType(typeInfo);
		return this;
	}

	/**
	 * Adds a type information hint about the return type of this operator.
	 *
	 * <p>
	 * Type hints are important in cases where the Java compiler
	 * throws away generic type information necessary for efficient execution.
	 *
	 * <p>
	 * This method takes a type information string that will be parsed. A type information string can contain the following
	 * types:
	 *
	 * <ul>
	 * <li>Basic types such as <code>Integer</code>, <code>String</code>, etc.
	 * <li>Basic type arrays such as <code>Integer[]</code>,
	 * <code>String[]</code>, etc.
	 * <li>Tuple types such as <code>Tuple1&lt;TYPE0&gt;</code>,
	 * <code>Tuple2&lt;TYPE0, TYPE1&gt;</code>, etc.</li>
	 * <li>Pojo types such as <code>org.my.MyPojo&lt;myFieldName=TYPE0,myFieldName2=TYPE1&gt;</code>, etc.</li>
	 * <li>Generic types such as <code>java.lang.Class</code>, etc.
	 * <li>Custom type arrays such as <code>org.my.CustomClass[]</code>,
	 * <code>org.my.CustomClass$StaticInnerClass[]</code>, etc.
	 * <li>Value types such as <code>DoubleValue</code>,
	 * <code>StringValue</code>, <code>IntegerValue</code>, etc.</li>
	 * <li>Tuple array types such as <code>Tuple2&lt;TYPE0,TYPE1&gt;[], etc.</code></li>
	 * <li>Writable types such as <code>Writable&lt;org.my.CustomWritable&gt;</code></li>
	 * <li>Enum types such as <code>Enum&lt;org.my.CustomEnum&gt;</code></li>
	 * </ul>
	 *
	 * Example:
	 * <code>"Tuple2&lt;String,Tuple2&lt;Integer,org.my.MyJob$Pojo&lt;word=String&gt;&gt;&gt;"</code>
	 *
	 * @param typeInfoString
	 *            type information string to be parsed
	 * @return This operator with a given return type hint.
	 *
	 * @deprecated Please use {@link #returns(Class)} or {@link #returns(TypeHint)} instead.
	 */
	@Deprecated
	@PublicEvolving
	public DataStreamSource<T> returns(String typeInfoString) {
		if (typeInfoString == null) {
			throw new IllegalArgumentException("Type information string must not be null.");
		}
		return returns(TypeInfoParser.<T>parse(typeInfoString));
	}

	/**
	 * Sets the {@link ChainingStrategy} for the given operator affecting the
	 * way operators will possibly be co-located on the same thread for
	 * increased performance.
	 *
	 * @param strategy
	 *            The selected {@link ChainingStrategy}
	 * @return The operator with the modified chaining strategy
	 */
	@PublicEvolving
	private DataStreamSource<T> setChainingStrategy(ChainingStrategy strategy) {
		this.transformation.setChainingStrategy(strategy);
		return this;
	}

	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization.
	 * <p> Chaining can be turned off for the whole
	 * job by {@link StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 *
	 * @return The operator with chaining disabled
	 */
	@PublicEvolving
	public DataStreamSource<T> disableChaining() {
		return setChainingStrategy(ChainingStrategy.NEVER);
	}

	/**
	 * Sets the slot sharing group of this operation. Parallel instances of
	 * operations that are in the same slot sharing group will be co-located in the same
	 * TaskManager slot, if possible.
	 *
	 * <p>Operations inherit the slot sharing group of input operations if all input operations
	 * are in the same slot sharing group and no slot sharing group was explicitly specified.
	 *
	 * <p>Initially an operation is in the default slot sharing group. An operation can be put into
	 * the default group explicitly by setting the slot sharing group to {@code "default"}.
	 *
	 * @param slotSharingGroup The slot sharing group name.
	 */
	@PublicEvolving
	public DataStreamSource<T> slotSharingGroup(String slotSharingGroup) {
		transformation.setSlotSharingGroup(slotSharingGroup);
		return this;
	}

	/**
	 * Sets an ID for this operator.
	 *
	 * <p>The specified ID is used to assign the same operator ID across job
	 * submissions (for example when starting a job from a savepoint).
	 *
	 * <p><strong>Important</strong>: this ID needs to be unique per
	 * transformation and job. Otherwise, job submission will fail.
	 *
	 * @param uid The unique user-specified ID of this transformation.
	 * @return The operator with the specified ID.
	 */
	@PublicEvolving
	public DataStreamSource<T> uid(String uid) {
		transformation.setUid(uid);
		return this;
	}

}

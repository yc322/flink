package org.apache.flink.streaming.api.sources.runtime;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.sources.Source;
import org.apache.flink.streaming.api.sources.SplitReader;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.ArrayList;
import java.util.List;

/**
 * Example implementation, this should be redone properly as an operator or StreamTask.
 */
public class SourceSplitReader<T, SplitT>
		extends AbstractStreamOperator<T>
		implements OneInputStreamOperator<SplitT, T>, ProcessingTimeCallback {

	private final Source<T, SplitT, ?> source;

	private final ListStateDescriptor<SplitT> splitStateDescriptor;

	private transient ListState<SplitT> splitState;

	private transient List<SplitReader<T, SplitT>> splits;

	public SourceSplitReader(Source<T, SplitT, ?> source) {
		this.source = source;
		splitStateDescriptor =
				new ListStateDescriptor<>("split-state", source.getSplitSerializer());
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		splitState = context.getOperatorStateStore().getListState(splitStateDescriptor);
		splits = new ArrayList<>();

		for (SplitT split : splitState.get()) {
			SplitReader<T, SplitT> splitReader = source.createSplitReader(split);
			boolean hasElement = splitReader.start();
			if (hasElement) {
				T currentElement = splitReader.getCurrent();
				output.collect(new StreamRecord<>(currentElement));
			}
			splits.add(splitReader);
		}

		ProcessingTimeService processingTimeService = getProcessingTimeService();
		processingTimeService
				.registerTimer(processingTimeService.getCurrentProcessingTime(), this);
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		splitState.clear();
		for (SplitReader<T, SplitT> splitReader : splits) {
			splitState.add(splitReader.checkpoint());
		}
	}

	@Override
	public void processElement(StreamRecord<SplitT> element) throws Exception {
		// obviously, we would read more than one element from the split here, but the concept
		// is clear, I hope ...

		SplitReader<T, SplitT> splitReader = source.createSplitReader(element.getValue());
		boolean hasElement = splitReader.start();
		if (hasElement) {
			T currentElement = splitReader.getCurrent();
			output.collect(new StreamRecord<>(currentElement));
		}
		splits.add(splitReader);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {

		// obviously, we would read more than one element from the split here, but the concept
		// is clear, I hope ...

		for (SplitReader<T, SplitT> splitReader : splits) {
			boolean hasElement = splitReader.advance();
			if (hasElement) {
				T currentElement = splitReader.getCurrent();
				output.collect(new StreamRecord<>(currentElement));
			}

			int numRecords = 0;
			// TODO: this is set very high for max throughput in local testing
			while (numRecords < 500_000) {
				if (!splitReader.advance()) {
					break;
				}
				T currentElement = splitReader.getCurrent();
				output.collect(new StreamRecord<>(currentElement));

				numRecords++;
			}
		}

		ProcessingTimeService processingTimeService = getProcessingTimeService();
		processingTimeService
				.registerTimer(processingTimeService.getCurrentProcessingTime(), this);
	}
}

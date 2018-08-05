package org.apache.flink.streaming.api.sources.runtime;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.sources.Source;
import org.apache.flink.streaming.api.sources.SplitEnumerator;

import java.util.Iterator;

/**
 * Example implementation, this should be redone properly as an operator or StreamTask.
 */
public class SourceEnumeratorAdaptor<T, SplitT, CheckpointT> implements SourceFunction<SplitT>, CheckpointedFunction {

	private volatile boolean running = true;

	private final Source<T, SplitT, CheckpointT> source;
	private final ListStateDescriptor<CheckpointT> stateDescriptor;

	private transient ListState<CheckpointT> listState;
	private transient SplitEnumerator<SplitT, CheckpointT> splitEnumerator;
	private transient CheckpointT enumeratorCheckpoint;

	public SourceEnumeratorAdaptor(
			Source<T, SplitT, CheckpointT> source) {
		this.source = source;
		this.stateDescriptor =
				new ListStateDescriptor<>("enumerator-listState", source.getEnumeratorCheckpointSerializer());
	}

	@Override
	public void run(SourceContext<SplitT> ctx) throws Exception {

		splitEnumerator = source.createSplitEnumerator(enumeratorCheckpoint);

		Object checkpointLock = ctx.getCheckpointLock();

		while (running) {
			synchronized (checkpointLock) {
				Iterable<SplitT> newSplits = splitEnumerator.discoverNewSplits();
				for (SplitT split : newSplits) {
					ctx.collect(split);
				}
			}
			Thread.sleep(20_000);
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		listState.clear();
		listState.add(splitEnumerator.checkpoint());
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		listState = context.getOperatorStateStore().getUnionListState(stateDescriptor);

		Iterator<CheckpointT> stateIterator = listState.get().iterator();
		if (stateIterator.hasNext()) {
			enumeratorCheckpoint = listState.get().iterator().next();
		} else {
			enumeratorCheckpoint = source.createInitialEnumeratorCheckpoint();
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}

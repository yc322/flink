package org.apache.flink.streaming.api.sources;

public interface SplitEnumerator<SplitT, CheckpointT> {
	Iterable<SplitT> discoverNewSplits();

	CheckpointT checkpoint();
}

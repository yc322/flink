package org.apache.flink.streaming.api.sources;

import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;

public interface Source<T, SplitT, EnumeratorCheckpointT> extends Serializable {
	TypeSerializer<SplitT> getSplitSerializer();

	TypeSerializer<T> getElementSerializer();

	TypeSerializer<EnumeratorCheckpointT> getEnumeratorCheckpointSerializer();

	EnumeratorCheckpointT createInitialEnumeratorCheckpoint();

	SplitEnumerator<SplitT, EnumeratorCheckpointT> createSplitEnumerator(EnumeratorCheckpointT checkpoint);

	SplitReader<T, SplitT> createSplitReader(SplitT split);
}

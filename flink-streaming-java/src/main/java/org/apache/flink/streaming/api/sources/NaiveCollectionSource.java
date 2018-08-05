package org.apache.flink.streaming.api.sources;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class NaiveCollectionSource<T> implements Source<T, List<T>, List<T>>, Serializable {

	private final TypeSerializer<T> elementSerializer;

	private final List<T> elements;

	public NaiveCollectionSource(TypeSerializer<T> elementSerializer, List<T> elements) {
		this.elementSerializer = elementSerializer;
		this.elements = elements;
	}

	@Override
	public TypeSerializer<List<T>> getSplitSerializer() {
		return new ListSerializer<>(elementSerializer);
	}

	@Override
	public TypeSerializer<T> getElementSerializer() {
		return elementSerializer;
	}

	@Override
	public TypeSerializer<List<T>> getEnumeratorCheckpointSerializer() {
		return new ListSerializer<>(elementSerializer);
	}

	@Override
	public List<T> createInitialEnumeratorCheckpoint() {
		return elements;
	}

	@Override
	public SplitEnumerator<List<T>, List<T>> createSplitEnumerator(List<T> checkpoint) {
		return new ListSplitEnumerator<>(checkpoint);
	}

	@Override
	public SplitReader<T, List<T>> createSplitReader(List<T> split) {
		return new ListSplitReader<>(split);
	}

	private static class ListSplitEnumerator<T> implements SplitEnumerator<List<T>, List<T>> {
		private List<T> elements;

		public ListSplitEnumerator(List<T> elements) {
			this.elements = elements;
		}

		@Override
		public Iterable<List<T>> discoverNewSplits() {
			if (elements.size() > 0) {
				// we always return splits of size 1
				T head = elements.remove(0);
				List<T> split = Collections.singletonList(head);
				return Collections.singletonList(split);
			} else {
				return Collections.emptyList();
			}
		}

		@Override
		public List<T> checkpoint() {
			return elements;
		}
	}

	private static class ListSplitReader<T> implements SplitReader<T, List<T>> {

		private List<T> elements;

		public ListSplitReader(List<T> elements) {
			this.elements = elements;
		}

		@Override
		public boolean start() throws IOException {
			return elements.size() > 0;
		}

		@Override
		public boolean advance() throws IOException {
			if (elements.size() > 0) {
				// pop one and check if we still have any
				elements.remove(0);
				return elements.size() > 0;
			}
			return false;
		}

		@Override
		public T getCurrent() throws NoSuchElementException {
			if (elements.size() <= 0) {
				throw new NoSuchElementException();
			}
			return elements.get(0);
		}

		@Override
		public long getCurrentTimestamp() throws NoSuchElementException {
			return Long.MIN_VALUE;
		}

		@Override
		public long getWatermark() {
			return Long.MIN_VALUE;
		}

		@Override
		public List<T> checkpoint() {
			return elements;
		}

		@Override
		public boolean isDone() {
			return elements.size() <= 0;
		}

		@Override
		public void close() throws IOException {

		}
	}
}

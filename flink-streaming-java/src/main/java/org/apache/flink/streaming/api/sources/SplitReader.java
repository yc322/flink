package org.apache.flink.streaming.api.sources;

import java.io.IOException;
import java.util.NoSuchElementException;

public interface SplitReader<T, SplitT> {

	boolean start() throws IOException;

	boolean advance() throws IOException;

	T getCurrent() throws NoSuchElementException;

	long getCurrentTimestamp() throws NoSuchElementException;

	long getWatermark();

	SplitT checkpoint();

	boolean isDone() throws IOException;

	void close() throws IOException;
}

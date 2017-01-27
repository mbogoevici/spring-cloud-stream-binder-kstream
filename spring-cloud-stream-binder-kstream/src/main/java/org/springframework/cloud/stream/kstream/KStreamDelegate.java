/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * @author Marius Bogoevici
 */
public class KStreamDelegate<K, V> implements KStream<K, V> {

	private volatile KStream<K, V> delegate;

	public void setDelegate(KStream<K, V> delegate) {
		this.delegate = delegate;
	}

	@Override
	public KStream<K, V> filter(Predicate<K, V> predicate) {
		return delegate.filter(predicate);
	}

	@Override
	public KStream<K, V> filterNot(Predicate<K, V> predicate) {
		return delegate.filterNot(predicate);
	}

	@Override
	public <K1> KStream<K1, V> selectKey(KeyValueMapper<K, V, K1> mapper) {
		return delegate.selectKey(mapper);
	}

	@Override
	public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper) {
		return delegate.map(mapper);
	}

	@Override
	public <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper) {
		return delegate.mapValues(mapper);
	}

	@Override
	public void print() {
		delegate.print();
	}

	@Override
	public void print(Serde<K> keySerde, Serde<V> valSerde) {
		delegate.print(keySerde, valSerde);
	}

	@Override
	public void writeAsText(String filePath) {
		delegate.writeAsText(filePath);
	}

	@Override
	public void writeAsText(String filePath, Serde<K> keySerde, Serde<V> valSerde) {
		delegate.writeAsText(filePath, keySerde, valSerde);
	}

	@Override
	public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper) {
		return delegate.flatMap(mapper);
	}

	@Override
	public <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor) {
		return delegate.flatMapValues(processor);
	}

	@Override
	public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
		return delegate.branch(predicates);
	}

	@Override
	public KStream<K, V> through(String topic) {
		return delegate.through(topic);
	}

	@Override
	public void foreach(ForeachAction<K, V> action) {
		delegate.foreach(action);
	}

	@Override
	public KStream<K, V> through(StreamPartitioner<K, V> partitioner, String topic) {
		return delegate.through(partitioner, topic);
	}

	@Override
	public KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic) {
		return delegate.through(keySerde, valSerde, topic);
	}

	@Override
	public KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic) {
		return delegate.through(keySerde, valSerde, partitioner, topic);
	}

	@Override
	public void to(String topic) {
		delegate.to(topic);
	}

	@Override
	public void to(StreamPartitioner<K, V> partitioner, String topic) {
		delegate.to(partitioner, topic);
	}

	@Override
	public void to(Serde<K> keySerde, Serde<V> valSerde, String topic) {
		delegate.to(keySerde, valSerde, topic);
	}

	@Override
	public void to(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic) {
		delegate.to(keySerde, valSerde, partitioner, topic);
	}

	@Override
	public <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames) {
		return delegate.transform(transformerSupplier, stateStoreNames);
	}

	@Override
	public <R> KStream<K, R> transformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier, String... stateStoreNames) {
		return delegate.transformValues(valueTransformerSupplier, stateStoreNames);
	}

	@Override
	public void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames) {
		delegate.process(processorSupplier, stateStoreNames);
	}

	@Override
	public <V1, R> KStream<K, R> join(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows, Serde<K> keySerde, Serde<V> thisValueSerde, Serde<V1> otherValueSerde) {
		return delegate.join(otherStream, joiner, windows, keySerde, thisValueSerde, otherValueSerde);
	}

	@Override
	public <V1, R> KStream<K, R> join(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
		return delegate.join(otherStream, joiner, windows);
	}

	@Override
	public <V1, R> KStream<K, R> outerJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows, Serde<K> keySerde, Serde<V> thisValueSerde, Serde<V1> otherValueSerde) {
		return delegate.outerJoin(otherStream, joiner, windows, keySerde, thisValueSerde, otherValueSerde);
	}

	@Override
	public <V1, R> KStream<K, R> outerJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
		return delegate.outerJoin(otherStream, joiner, windows);
	}

	@Override
	public <V1, R> KStream<K, R> leftJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows, Serde<K> keySerde, Serde<V1> otherValueSerde) {
		return delegate.leftJoin(otherStream, joiner, windows, keySerde, otherValueSerde);
	}

	@Override
	public <V1, R> KStream<K, R> leftJoin(KStream<K, V1> otherStream, ValueJoiner<V, V1, R> joiner, JoinWindows windows) {
		return delegate.leftJoin(otherStream, joiner, windows);
	}

	@Override
	public <V1, V2> KStream<K, V2> leftJoin(KTable<K, V1> table, ValueJoiner<V, V1, V2> joiner) {
		return delegate.leftJoin(table, joiner);
	}

	@Override
	public <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows, Serde<K> keySerde, Serde<V> valueSerde) {
		return delegate.reduceByKey(reducer, windows, keySerde, valueSerde);
	}

	@Override
	public <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows) {
		return delegate.reduceByKey(reducer, windows);
	}

	@Override
	public KTable<K, V> reduceByKey(Reducer<V> reducer, Serde<K> keySerde, Serde<V> valueSerde, String name) {
		return delegate.reduceByKey(reducer, keySerde, valueSerde, name);
	}

	@Override
	public KTable<K, V> reduceByKey(Reducer<V> reducer, String name) {
		return delegate.reduceByKey(reducer, name);
	}

	@Override
	public <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer, Aggregator<K, V, T> aggregator, Windows<W> windows, Serde<K> keySerde, Serde<T> aggValueSerde) {
		return delegate.aggregateByKey(initializer, aggregator, windows, keySerde, aggValueSerde);
	}

	@Override
	public <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer, Aggregator<K, V, T> aggregator, Windows<W> windows) {
		return delegate.aggregateByKey(initializer, aggregator, windows);
	}

	@Override
	public <T> KTable<K, T> aggregateByKey(Initializer<T> initializer, Aggregator<K, V, T> aggregator, Serde<K> keySerde, Serde<T> aggValueSerde, String name) {
		return delegate.aggregateByKey(initializer, aggregator, keySerde, aggValueSerde, name);
	}

	@Override
	public <T> KTable<K, T> aggregateByKey(Initializer<T> initializer, Aggregator<K, V, T> aggregator, String name) {
		return delegate.aggregateByKey(initializer, aggregator, name);
	}

	@Override
	public <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows, Serde<K> keySerde) {
		return delegate.countByKey(windows, keySerde);
	}

	@Override
	public <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows) {
		return delegate.countByKey(windows);
	}

	@Override
	public KTable<K, Long> countByKey(Serde<K> keySerde, String name) {
		return delegate.countByKey(keySerde, name);
	}

	@Override
	public KTable<K, Long> countByKey(String name) {
		return delegate.countByKey(name);
	}
}

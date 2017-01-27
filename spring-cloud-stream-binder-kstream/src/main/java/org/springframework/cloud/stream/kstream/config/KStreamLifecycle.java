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

package org.springframework.cloud.stream.kstream.config;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import org.springframework.context.SmartLifecycle;

/**
 * @author Marius Bogoevici
 */
public class KStreamLifecycle implements SmartLifecycle {

	private KafkaStreams kafkaStreams;

	private TopologyBuilder topologyBuilder;

	private final KStreamBinderProperties kStreamBinderProperties;

	public KStreamLifecycle(TopologyBuilder topologyBuilder, KStreamBinderProperties kStreamBinderProperties) {
		this.topologyBuilder = topologyBuilder;
		this.kStreamBinderProperties = kStreamBinderProperties;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable runnable) {
		this.kafkaStreams.close();
		if (runnable != null) {
			runnable.run();
		}
	}

	@Override
	public void start() {
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
				Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
				Serdes.ByteArraySerde.class.getName());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "group2");
		props.putAll(kStreamBinderProperties.getConfiguration());
		this.kafkaStreams = new KafkaStreams(topologyBuilder, props);
		this.kafkaStreams.start();

	}

	@Override
	public void stop() {
		stop(null);
	}

	@Override
	public boolean isRunning() {
		return false;
	}

	@Override
	public int getPhase() {
		return Integer.MAX_VALUE - 500;
	}
}

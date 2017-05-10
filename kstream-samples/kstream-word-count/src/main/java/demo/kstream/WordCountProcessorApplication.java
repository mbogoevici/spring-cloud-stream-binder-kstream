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

package demo.kstream;

import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.kstream.annotations.KStreamProcessor;
import org.springframework.messaging.handler.annotation.SendTo;

/**
 * @author Marius Bogoevici
 */
@SpringBootApplication
@EnableBinding(KStreamProcessor.class)
@EnableConfigurationProperties(WordCountProcessorProperties.class)
public class WordCountProcessorApplication {

	@Autowired
	private WordCountProcessorProperties processorProperties;

	@StreamListener("input")
	@SendTo("output")
	public KStream<?, WordCount> process(KStream<?, String> input) {
		return input
				.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
				.map((key, word) -> new KeyValue<>(word, word))
				.groupByKey(Serdes.String(), Serdes.String())
				.count(configuredTimeWindow(), processorProperties.getStoreName())
				.toStream()
				.map((w, c) -> new KeyValue<>(null, new WordCount(w.key(), c, new Date(w.window().start()), new Date(w.window().end()))));
	}

	/**
	 * Constructs a {@link TimeWindows} property.
	 * @return
	 */
	private TimeWindows configuredTimeWindow() {
		return processorProperties.getAdvanceBy() > 0
				? TimeWindows.of(processorProperties.getWindowLength()).advanceBy(processorProperties.getAdvanceBy())
				: TimeWindows.of(processorProperties.getWindowLength());
	}

	public static void main(String[] args) {
		SpringApplication.run(WordCountProcessorApplication.class, args);
	}

}

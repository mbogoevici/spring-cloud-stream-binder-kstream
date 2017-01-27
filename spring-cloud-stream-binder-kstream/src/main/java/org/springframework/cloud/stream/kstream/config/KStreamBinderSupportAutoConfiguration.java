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

import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TopologyBuilder;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.kstream.KStreamBoundElementFactory;
import org.springframework.cloud.stream.kstream.KStreamListenerParameterAdapter;
import org.springframework.cloud.stream.kstream.KStreamStreamListenerResultAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.codec.Codec;

/**
 * @author Marius Bogoevici
 */

@EnableBinding
@EnableConfigurationProperties(KStreamBinderProperties.class)
public class KStreamBinderSupportAutoConfiguration {

    @Bean
    public KStreamBuilder kStreamBuilder() {
        return new KStreamBuilder();
    }

    @Bean
    public KStreamStreamListenerResultAdapter kStreamStreamListenerResultAdapter() {
        return new KStreamStreamListenerResultAdapter();
    }

    @Bean
    public KStreamListenerParameterAdapter kStreamListenerParameterAdapter(CompositeMessageConverterFactory compositeMessageConverterFactory) {
        return new KStreamListenerParameterAdapter(compositeMessageConverterFactory.getMessageConverterForAllRegistered());
    }

    @Bean
    public KStreamBoundElementFactory kStreamBindableTargetFactory(KStreamBuilder kStreamBuilder,
                                                                   BindingServiceProperties bindingServiceProperties,
                                                                   Codec codec,
                                                                   CompositeMessageConverterFactory compositeMessageConverterFactory) {
        return new KStreamBoundElementFactory(kStreamBuilder, bindingServiceProperties, codec, compositeMessageConverterFactory);
    }

    @Bean
    public KStreamLifecycle kStreamLifecycle(TopologyBuilder topologyBuilder, KStreamBinderProperties kStreamBinderProperties) {
        return new KStreamLifecycle(topologyBuilder, kStreamBinderProperties);
    }
}

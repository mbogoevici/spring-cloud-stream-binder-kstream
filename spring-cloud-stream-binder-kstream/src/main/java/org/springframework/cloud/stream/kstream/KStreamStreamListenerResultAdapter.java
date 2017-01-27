package org.springframework.cloud.stream.kstream;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

/**
 * @author Marius Bogoevici
 */
public class KStreamStreamListenerResultAdapter implements StreamListenerResultAdapter<KStream, KStream> {
    @Override
    public boolean supports(Class<?> resultType, Class<?> boundElement) {
        return KStream.class.isAssignableFrom(resultType) && KStream.class.isAssignableFrom(boundElement);
    }

    @Override
    public void adapt(KStream streamListenerResult, KStream boundElement) {
        ((KStreamDelegate<?,?>) boundElement).setDelegate(streamListenerResult.map((k,v) -> {
            if (v instanceof Message<?>) {
                return new KeyValue<>(k,v);
            }
            else {
                return new KeyValue<>(k, MessageBuilder.withPayload(v).build());
            }
        }));
    }
}

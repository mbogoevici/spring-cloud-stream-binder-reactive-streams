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

package org.springframework.cloud.stream.binder.kafka.reactive;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.Receiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;

import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.reactivestreams.ReactiveStreamsBinder;
import org.springframework.cloud.stream.reactive.FluxSender;

/**
 * @author Marius Bogoevici
 */
public class ReactiveKafkaBinder extends ReactiveStreamsBinder {

	@Override
	protected Flux<?> createConsumerFlux(String name, ConsumerProperties consumerProperties) {
		Properties configProperties = new Properties();
		configProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		configProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		Flux<ReceiverRecord<Object, Object>> dataFlux =
				Receiver.create(ReceiverOptions.create(configProperties)
										.subscription(Collections.singleton(name))).receive();
		return dataFlux.map(rr -> rr.record().value());
	}

	@Override
	protected FluxSender createProducerFluxSender(String name, ProducerProperties producerProperties) {
		Properties configProperties = new Properties();
		configProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		configProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return flux -> Sender.create(SenderOptions.create(configProperties))
				.outbound()
				.send(flux.map(v -> new ProducerRecord<>(name, v))).then();
	}
}

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

package org.springframework.cloud.stream.binder.reactivestreams;


import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.Receiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;

import org.springframework.cloud.stream.binding.BindingTargetFactory;

/**
 * @author Marius Bogoevici
 */
public class BoundFluxFactory implements BindingTargetFactory {

	public boolean canCreate(Class<?> clazz) {
		return Publisher.class.isAssignableFrom(clazz);
	}

	public Object createInput(String name) {
		Properties configProperties = new Properties();
		configProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		configProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		configProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		Flux<ReceiverRecord<Object, Object>> dataFlux = Receiver.create(ReceiverOptions.create(configProperties).subscription(Collections.singleton(name))).receive();
		return dataFlux.map(rr -> rr.record().value());
	}

	public Object createOutput(String name) {
		throw new UnsupportedOperationException();
	}

}

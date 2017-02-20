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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.Receiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;

import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaConsumerProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaExtendedBindingProperties;
import org.springframework.cloud.stream.binder.kafka.properties.KafkaProducerProperties;
import org.springframework.cloud.stream.binder.kafka.provisioning.KafkaTopicProvisioner;
import org.springframework.cloud.stream.binder.reactivestreams.ReactiveStreamsBinder;
import org.springframework.cloud.stream.reactive.FluxSender;
import org.springframework.integration.codec.Codec;
import org.springframework.messaging.MessageChannel;

/**
 * @author Marius Bogoevici
 */
public class ReactiveKafkaBinder extends ReactiveStreamsBinder <ExtendedConsumerProperties<KafkaConsumerProperties>,
																	   ExtendedProducerProperties<KafkaProducerProperties>>
		implements ExtendedPropertiesBinder<Publisher, KafkaConsumerProperties, KafkaProducerProperties> {

	private final KafkaTopicProvisioner kafkaTopicProvisioner;

	private final KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties;

	private final KafkaExtendedBindingProperties kafkaExtendedBindingProperties
			;
	private final Codec codec;

	public ReactiveKafkaBinder(KafkaTopicProvisioner kafkaTopicProvisioner,
							   KafkaBinderConfigurationProperties kafkaBinderConfigurationProperties,
							   KafkaExtendedBindingProperties kafkaExtendedBindingProperties, Codec codec) {

		this.kafkaTopicProvisioner = kafkaTopicProvisioner;
		this.kafkaBinderConfigurationProperties = kafkaBinderConfigurationProperties;
		this.kafkaExtendedBindingProperties = kafkaExtendedBindingProperties;
		this.codec = codec;
	}

	@Override
	protected Flux<?> createConsumerFlux(String name, String group, ExtendedConsumerProperties<KafkaConsumerProperties> consumerProperties) {
		this.kafkaTopicProvisioner.provisionConsumerDestination(name, group, consumerProperties);
		Properties configProperties = new Properties();
		configProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBinderConfigurationProperties.getKafkaConnectionString());
		configProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		configProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		configProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		configProperties.putAll(consumerProperties.getExtension().getConfiguration());
		Flux<ReceiverRecord<Object, Object>> dataFlux =
				Receiver.create(ReceiverOptions.create(configProperties)
										.subscription(Collections.singleton(name))).receive();
		return dataFlux.map(rr -> rr.record().value());
	}

	@Override
	protected FluxSender createProducerFluxSender(String name, ExtendedProducerProperties<KafkaProducerProperties> producerProperties) {
		this.kafkaTopicProvisioner.provisionProducerDestination(name,  producerProperties);
		Properties configProperties = new Properties();
		configProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBinderConfigurationProperties.getKafkaConnectionString());
		configProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		configProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		configProperties.putAll(producerProperties.getExtension().getConfiguration());
		return flux -> Sender.create(SenderOptions.create(configProperties))
				.outbound()
				.send(flux.map(v -> new ProducerRecord<>(name, v))).then();
	}

	@Override
	public KafkaConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.kafkaExtendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public KafkaProducerProperties getExtendedProducerProperties(String channelName) {
		return this.kafkaExtendedBindingProperties.getExtendedProducerProperties(channelName);
	}
}

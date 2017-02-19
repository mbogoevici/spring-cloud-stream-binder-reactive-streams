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

import org.reactivestreams.Publisher;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.context.Lifecycle;

/**
 * @author Marius Bogoevici
 */
public class ReactiveStreamsBinder extends AbstractBinder<Publisher, ConsumerProperties, ProducerProperties> {

	@Override
	protected Binding<Publisher> doBindConsumer(String name, String group, Publisher inputTarget, ConsumerProperties properties) {

		//inputTarget.publish().autoConnect().log();
		return new DefaultBinding<>(name, group, inputTarget, new Lifecycle() {
			@Override
			public void start() {
			}

			@Override
			public void stop() {
			}

			@Override
			public boolean isRunning() {
				return false;
			}
		});
	}

	@Override
	protected Binding<Publisher> doBindProducer(String name, Publisher outboundBindTarget, ProducerProperties properties) {
		throw new UnsupportedOperationException();
//		Properties configProperties = new Properties();
//		configProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//		configProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		configProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//		Disposable subscribe = Sender
//									   .create(SenderOptions.create(configProperties))
//									   .outbound()
//									   .send(((DefaultOutputPublisher)outboundBindTarget).getInternalFlux().log().map(x -> new ProducerRecord<>(name, x)))
//									   .then().subscribe();
//		return new DefaultBinding<>(name, null, outboundBindTarget, new Lifecycle() {
//			@Override
//			public void start() {
//			}
//
//			@Override
//			public void stop() {
//				subscribe.dispose();
//			}
//
//			@Override
//			public boolean isRunning() {
//				return false;
//			}
//		});
	}
}

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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.reactivestreams.factory.FluxSenderPublisher;
import org.springframework.cloud.stream.reactive.FluxSender;
import org.springframework.context.Lifecycle;

/**
 * @author Marius Bogoevici
 */
public abstract class ReactiveStreamsBinder extends AbstractBinder<Publisher, ConsumerProperties, ProducerProperties> {

	@Override
	protected Binding<Publisher> doBindConsumer(String name, String group, Publisher inputTarget, ConsumerProperties properties) {
		Flux<?> map = createConsumerFlux(name, properties);
		Disposable subscription;
		if (inputTarget instanceof Processor) {
			subscription = map.doOnNext(o -> ((Processor)inputTarget).onNext(o)).subscribe();
		}
		else {
			throw new IllegalArgumentException("Expected a Processor instance to bind");
		}
		return new DefaultBinding<>(name, group, inputTarget, new Lifecycle() {
			@Override
			public void start() {
			}

			@Override
			public void stop() {
				subscription.dispose();
			}

			@Override
			public boolean isRunning() {
				return false;
			}
		});
	}

	protected abstract Flux<?> createConsumerFlux(String name, ConsumerProperties consumerProperties);

	@Override
	protected Binding<Publisher> doBindProducer(String name, Publisher outboundBindTarget, ProducerProperties producerProperties) {
		Flux<Object> outboundFlux = ((FluxSenderPublisher<Object>) outboundBindTarget).getInternalFlux();
		Disposable subscribe = createProducerFluxSender(name, producerProperties).send(outboundFlux).subscribe();
		return new DefaultBinding<>(name, null, outboundBindTarget, new Lifecycle() {
			@Override
			public void start() {
			}

			@Override
			public void stop() {
				subscribe.dispose();
			}

			@Override
			public boolean isRunning() {
				return false;
			}
		});
	}

	protected abstract FluxSender createProducerFluxSender(String name, ProducerProperties producerProperties);
}

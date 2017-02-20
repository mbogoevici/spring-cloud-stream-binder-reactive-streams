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

package org.springframework.cloud.stream.binder.reactivestreams.factory;

import java.util.Collections;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.PartitionHandler;
import org.springframework.cloud.stream.binder.PartitionKeyExtractorStrategy;
import org.springframework.cloud.stream.binder.PartitionSelectorStrategy;
import org.springframework.cloud.stream.binding.BindingTargetFactory;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.reactive.FluxSender;
import org.springframework.integration.expression.ExpressionUtils;
import org.springframework.integration.support.MessageBuilderFactory;
import org.springframework.integration.support.MutableMessageBuilderFactory;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;
import org.springframework.util.StringUtils;

/**
 * @author Marius Bogoevici
 */
public class FluxSenderFactory implements BindingTargetFactory, BeanFactoryAware {

	private final BindingServiceProperties bindingServiceProperties;

	private final CompositeMessageConverterFactory compositeMessageConverterFactory;

	private BeanFactory beanFactory;

	private final MessageBuilderFactory messageBuilderFactory = new MutableMessageBuilderFactory();

	public FluxSenderFactory(BindingServiceProperties bindingServiceProperties, CompositeMessageConverterFactory compositeMessageConverterFactory) {
		this.bindingServiceProperties = bindingServiceProperties;
		this.compositeMessageConverterFactory = compositeMessageConverterFactory;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
	}

	@Override
	public boolean canCreate(Class<?> clazz) {
		return FluxSender.class.isAssignableFrom(clazz);
	}

	@Override
	public Object createInput(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object createOutput(String name) {

		BindingProperties bindingProperties = bindingServiceProperties.getBindingProperties(name);
		if (StringUtils.hasText(bindingProperties.getContentType())) {
			final MessageConverter messageConverter =
					compositeMessageConverterFactory.getMessageConverterForType(MimeType.valueOf(bindingProperties.getContentType()));
			return new ConvertingFluxSender(messageConverter, bindingProperties, new FluxSenderPublisher<>());

		} else {
			return new FluxSenderPublisher();
		}
	}

	private static class ConvertingFluxSender implements FluxSender {

		private final MessageConverter messageConverter;
		private final BindingProperties bindingProperties;

		private final FluxSender delegate;

		public ConvertingFluxSender(MessageConverter messageConverter, BindingProperties bindingProperties, FluxSender delegate) {
			this.messageConverter = messageConverter;
			this.bindingProperties = bindingProperties;
			this.delegate = delegate;
		}

		@Override
		public Mono<Void> send(Flux<?> flux) {
			return delegate.send(flux.map(o -> {
				if (o instanceof Message) {
					Message<?> message = (Message<?>) o;
					return messageConverter.toMessage(
							message.getPayload(),
							new MutableMessageHeaders(message.getHeaders()));
				} else {
					return messageConverter.toMessage(o,
							new MessageHeaders(Collections.singletonMap(MessageHeaders.CONTENT_TYPE,
									bindingProperties.getContentType())));
				}
			}));
		}
	}

	private class PartitioningFluxSender implements FluxSender {

		private final BindingProperties bindingProperties;

		private final PartitionHandler partitionHandler;

		private final FluxSender delegate;

		public PartitioningFluxSender(BindingProperties bindingProperties,
									  PartitionKeyExtractorStrategy partitionKeyExtractorStrategy,
									  PartitionSelectorStrategy partitionSelectorStrategy,
									  FluxSender delegate) {
			this.bindingProperties = bindingProperties;
			this.delegate = delegate;
			this.partitionHandler = new PartitionHandler(ExpressionUtils.createStandardEvaluationContext(FluxSenderFactory.this.beanFactory),
																this.bindingProperties.getProducer(), partitionKeyExtractorStrategy, partitionSelectorStrategy);

		}

		@Override
		public Mono<Void> send(Flux<?> flux) {
			return delegate.send(flux.map(m -> {
				int partition = partitionHandler.determinePartition((Message<?>) m);
				return FluxSenderFactory.this.messageBuilderFactory.fromMessage((Message<?>) m).setHeader(BinderHeaders.PARTITION_HEADER, partition).build();
			}));
		}
	}

}

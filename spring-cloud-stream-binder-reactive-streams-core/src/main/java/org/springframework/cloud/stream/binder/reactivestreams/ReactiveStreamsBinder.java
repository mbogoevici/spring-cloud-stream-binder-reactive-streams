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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import org.springframework.cloud.stream.binder.AbstractBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.DefaultBinding;
import org.springframework.cloud.stream.binder.EmbeddedHeadersMessageConverter;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.MessageValues;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binder.StringConvertingContentTypeResolver;
import org.springframework.cloud.stream.binder.reactivestreams.factory.FluxSenderPublisher;
import org.springframework.cloud.stream.converter.CompositeMessageConverterFactory;
import org.springframework.cloud.stream.reactive.FluxSender;
import org.springframework.context.Lifecycle;
import org.springframework.core.serializer.support.SerializationFailedException;
import org.springframework.integration.codec.Codec;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import org.springframework.util.ObjectUtils;

/**
 * @author Marius Bogoevici
 */
public abstract class ReactiveStreamsBinder extends AbstractBinder<Publisher, ConsumerProperties, ProducerProperties> {

	private volatile Codec codec;

	private final StringConvertingContentTypeResolver contentTypeResolver = new StringConvertingContentTypeResolver();

	private volatile Map<String, Class<?>> payloadTypeCache = new ConcurrentHashMap<>();

	private CompositeMessageConverterFactory compositeMessageConverterFactory;


	EmbeddedHeadersMessageConverter embeddedHeadersMessageConverter = new EmbeddedHeadersMessageConverter();

	@Override
	protected Binding<Publisher> doBindConsumer(String name, String group, Publisher inputTarget, ConsumerProperties properties) {
		Flux<?> map = createConsumerFlux(name, group, properties);
		Disposable subscription;
		Flux<?> effectiveInputTarget;
		if (inputTarget instanceof Processor) {
			if (HeaderMode.embeddedHeaders.equals(properties)) {
				effectiveInputTarget = Flux.from(inputTarget).map(v -> {
					if (!(v instanceof byte[])) {
						return v;
					}
					else {
						try {
							MessageValues messageValues = embeddedHeadersMessageConverter.extractHeaders((byte[]) v);
							messageValues = deserializePayloadIfNecessary(messageValues);
							return messageValues.toMessage();
						} catch (Exception e) {
							throw new IllegalArgumentException(e);
						}
					}
				});
			}
			else {
				effectiveInputTarget = Flux.from(inputTarget);
			}
			subscription = map.doOnNext(o -> ((Processor)effectiveInputTarget).onNext(o)).subscribe();
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

	protected abstract Flux<?> createConsumerFlux(String name, String group, ConsumerProperties consumerProperties);

	@Override
	protected Binding<Publisher> doBindProducer(String name, Publisher outboundBindTarget, ProducerProperties producerProperties) {
		Flux<Object> outboundFlux = ((FluxSenderPublisher<Object>) outboundBindTarget).getInternalFlux();
		outboundFlux = outboundFlux.map(m -> {
			try {
				return embeddedHeadersMessageConverter.embedHeaders(
						serializePayloadIfNecessary((Message<?>) m), BinderHeaders.STANDARD_HEADERS);
			}
			catch (Exception e) {
				return null;
			}
		}).filter(m -> m != null).cast(Object.class);
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

	private Object deserializePayload(Object payload, MimeType contentType) {
		if (payload instanceof byte[]) {
			if (contentType == null || MimeTypeUtils.APPLICATION_OCTET_STREAM.equals(contentType)) {
				return payload;
			} else {
				return deserializePayload((byte[]) payload, contentType);
			}
		}
		return payload;
	}

	private Object deserializePayload(byte[] bytes, MimeType contentType) {
		if ("text".equalsIgnoreCase(contentType.getType()) || MimeTypeUtils.APPLICATION_JSON.equals(contentType)) {
			try {
				return new String(bytes, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				String errorMessage = "unable to deserialize [java.lang.String]. Encoding not supported. " + e.getMessage();
				logger.error(errorMessage);
				throw new SerializationFailedException(errorMessage, e);
			}
		} else {
			String className = JavaClassMimeTypeConversion.classNameFromMimeType(contentType);
			try {
				// Cache types to avoid unnecessary ClassUtils.forName calls.
				Class<?> targetType = this.payloadTypeCache.get(className);
				if (targetType == null) {
					targetType = ClassUtils.forName(className, null);
					this.payloadTypeCache.put(className, targetType);
				}
				return this.codec.decode(bytes, targetType);
			}// catch all exceptions that could occur during de-serialization
			catch (Exception e) {
				String errorMessage = "Unable to deserialize [" + className + "] using the contentType [" + contentType + "] " + e.getMessage();
				logger.error(errorMessage);
				throw new SerializationFailedException(errorMessage, e);
			}
		}
	}


	public abstract static class JavaClassMimeTypeConversion {

		private static ConcurrentMap<String, MimeType> mimeTypesCache = new ConcurrentHashMap<>();

		static MimeType mimeTypeFromObject(Object payload, String originalContentType) {
			Assert.notNull(payload, "payload object cannot be null.");
			if (payload instanceof byte[]) {
				return MimeTypeUtils.APPLICATION_OCTET_STREAM;
			}
			if (payload instanceof String) {
				return MimeTypeUtils.APPLICATION_JSON_VALUE.equals(originalContentType) ? MimeTypeUtils.APPLICATION_JSON
							   : MimeTypeUtils.TEXT_PLAIN;
			}
			String className = payload.getClass().getName();
			MimeType mimeType = mimeTypesCache.get(className);
			if (mimeType == null) {
				String modifiedClassName = className;
				if (payload.getClass().isArray()) {
					// Need to remove trailing ';' for an object array, e.g.
					// "[Ljava.lang.String;" or multi-dimensional
					// "[[[Ljava.lang.String;"
					if (modifiedClassName.endsWith(";")) {
						modifiedClassName = modifiedClassName.substring(0, modifiedClassName.length() - 1);
					}
					// Wrap in quotes to handle the illegal '[' character
					modifiedClassName = "\"" + modifiedClassName + "\"";
				}
				mimeType = MimeType.valueOf("application/x-java-object;type=" + modifiedClassName);
				mimeTypesCache.put(className, mimeType);
			}
			return mimeType;
		}

		static String classNameFromMimeType(MimeType mimeType) {
			Assert.notNull(mimeType, "mimeType cannot be null.");
			String className = mimeType.getParameter("type");
			if (className == null) {
				return null;
			}
			// unwrap quotes if any
			className = className.replace("\"", "");

			// restore trailing ';'
			if (className.contains("[L")) {
				className += ";";
			}
			return className;
		}

	}

	final MessageValues serializePayloadIfNecessary(Message<?> message) {
		Object originalPayload = message.getPayload();
		Object originalContentType = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);

		// Pass content type as String since some transport adapters will exclude
		// CONTENT_TYPE Header otherwise
		Object contentType = JavaClassMimeTypeConversion
									 .mimeTypeFromObject(originalPayload, ObjectUtils.nullSafeToString(originalContentType)).toString();
		Object payload = serializePayloadIfNecessary(originalPayload);
		MessageValues messageValues = new MessageValues(message);
		messageValues.setPayload(payload);
		messageValues.put(MessageHeaders.CONTENT_TYPE, contentType);
		if (originalContentType != null && !originalContentType.toString().equals(contentType.toString())) {
			messageValues.put(BinderHeaders.BINDER_ORIGINAL_CONTENT_TYPE, originalContentType.toString());
		}
		return messageValues;
	}

	private byte[] serializePayloadIfNecessary(Object originalPayload) {
		if (originalPayload instanceof byte[]) {
			return (byte[]) originalPayload;
		}
		else {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				if (originalPayload instanceof String) {
					return ((String) originalPayload).getBytes("UTF-8");
				}
				this.codec.encode(originalPayload, bos);
				return bos.toByteArray();
			}
			catch (IOException e) {
				throw new SerializationFailedException(
															  "unable to serialize payload [" + originalPayload.getClass().getName() + "]", e);
			}
		}
	}
}

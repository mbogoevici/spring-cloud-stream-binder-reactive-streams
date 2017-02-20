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
import reactor.core.publisher.Flux;

import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.reactive.FluxSender;

/**
 * @author Marius Bogoevici
 */
public class ReactiveStreamsFluxSenderResultAdapter implements StreamListenerResultAdapter<Publisher, FluxSender> {

	@Override
	public boolean supports(Class<?> resultType, Class<?> bindingTarget) {
		return Publisher.class.isAssignableFrom(resultType)
				&& Publisher.class.isAssignableFrom(bindingTarget);
	}

	@Override
	public void adapt(Publisher streamListenerResult, FluxSender bindingTarget) {
		bindingTarget.send(Flux.from(streamListenerResult)).retry();
	}
}

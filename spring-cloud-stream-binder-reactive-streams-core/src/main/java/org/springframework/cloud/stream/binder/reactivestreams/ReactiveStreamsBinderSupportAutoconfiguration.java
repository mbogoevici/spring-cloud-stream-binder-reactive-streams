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

import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.annotation.Bean;

/**
 * @author Marius Bogoevici
 */
@ConditionalOnBean(BindingService.class)
public class ReactiveStreamsBinderSupportAutoconfiguration {

	@Bean
	public BoundFluxFactory boundFluxFactory() {
		return new BoundFluxFactory();
	}

	@Bean
	public ReactiveStreamsFluxResultAdapter fluxResultAdapter() {
		return new ReactiveStreamsFluxResultAdapter();
	}

	@Bean
	public ReactiveStreamsFluxToPublisherParameterAdapter fluxToPublisherParameterAdapter() {
		return new ReactiveStreamsFluxToPublisherParameterAdapter();
	}
}

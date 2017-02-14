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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.Receiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.Sender;
import reactor.kafka.sender.SenderOptions;

/**
 * @author Marius Bogoevici
 */
public class TestFlux {

	public static void main(String[] args) {
		Flux<Integer> originalFlux = Flux.just(1, 2, 3);

		Flux<Object> data = Flux.empty();

		data.concatWith(originalFlux).subscribe(System.out::println);


		Flux<ReceiverRecord<Object, Object>> dataFlux =
				Receiver.create(ReceiverOptions.create().subscription(Collections.singleton("input"))).receive()
				.map(x -> x);
		//Sender.create(SenderOptions.create()).send(originalFlux.map(r -> new ProducerRecord<>("output", "X"))).subscribe();
	}
}

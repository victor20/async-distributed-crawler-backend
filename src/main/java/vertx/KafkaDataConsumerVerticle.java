package vertx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class KafkaDataConsumerVerticle extends AbstractVerticle {

	@SuppressWarnings("deprecation")
	@Override
	public void start(Future<Void> startFuture) {
		Map<String, String> config = new HashMap<>();
		config.put("bootstrap.servers", "localhost:9092");
		config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		config.put("group.id", "my_group");
		config.put("auto.offset.reset", "latest");
		config.put("enable.auto.commit", "false");

		// use consumer for interacting with Apache Kafka
		KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);

		consumer.handler(record -> {
			String data = record.value();
			
			System.out.println("Processing key=" + record.key() + ",value=" + data + ",partition="
					+ record.partition() + ",offset=" + record.offset());
			//System.out.println(record.value().getClass());

			// Diagram diagram = Json.decodeValue(routingContext.getBodyAsString(),
			// Diagram.class);

			
			JsonObject message = new JsonObject().put("action", "add-data").put("data", data);

			vertx.<JsonObject>eventBus().send("db-adress", message, ar -> {

				if (ar.succeeded()) {
					System.out.println("Data added to DB");
				} else {
					//System.out.println("Data NOT added to DB");
				}
			});

		});

		/*
		 * // subscribe to several topics Set<String> topics = new HashSet<>();
		 * topics.add("data-topic"); topics.add("test topic");
		 * consumer.subscribe(topics, ar -> { if (ar.succeeded()) {
		 * System.out.println("subscribed"); } else {
		 * System.out.println("Could not subscribe " + ar.cause().getMessage()); } });
		 */

		// or just subscribe to a single topic
		consumer.subscribe("data-topic", ar -> {
			if (ar.succeeded()) {
				System.out.println("subscribed to data-topic");
			} else {
				System.out.println("Could not subscribe to data-topic " + ar.cause().getMessage());
			}
		});

	}
}

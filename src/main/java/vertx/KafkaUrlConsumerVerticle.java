package vertx;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;

public class KafkaUrlConsumerVerticle extends AbstractVerticle  {
	
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
			String url = record.value();  
			System.out.println("Processing key=" + record.key() + ",value=" + url +
			    ",partition=" + record.partition() + ",offset=" + record.offset());
			
			JsonObject message = new JsonObject().put("action", "add-url").put("url", url);

			vertx.<JsonObject>eventBus().send("db-adress", message, ar -> {

				if (ar.succeeded()) {
					System.out.println("Url added to DB");
					//Skicka tillbaka till crawler att denna ska crawlas
				} else {
					//System.out.println("Url NOT added to DB");
				}
			});
			
		});
		
		/*
		// subscribe to several topics
		Set<String> topics = new HashSet<>();
		topics.add("data-topic");
		topics.add("test topic");
		consumer.subscribe(topics, ar -> {
		  if (ar.succeeded()) {
		    System.out.println("subscribed");
		  } else {
		    System.out.println("Could not subscribe " + ar.cause().getMessage());
		  }
		});
		*/
		
		// or just subscribe to a single topic
		consumer.subscribe("url-topic", ar -> {
			if (ar.succeeded()) {
				System.out.println("subscribed to url-topic");
			} else {
				System.out.println("Could not subscribe to url-topic " + ar.cause().getMessage());
			}
		});
		
	}
}

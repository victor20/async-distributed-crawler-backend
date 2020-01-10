package vertx;

import java.util.HashMap;
import java.util.Map;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;

public class KafkaUrlProducerVerticle extends AbstractVerticle {

	KafkaProducer<String, String> producer;
	

	@Override
	public void start(Future<Void> startFuture) {
		
		SharedData topicNr = vertx.sharedData();
		LocalMap<String, String> map1 = topicNr.getLocalMap("mymap1");
		map1.put("topicNr", "0");
		
		Map<String, String> config = new HashMap<>();
		config.put("bootstrap.servers", "localhost:9092");
		config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		config.put("acks", "1");

		producer = KafkaProducer.create(vertx, config);

		EventBus eventBus = vertx.eventBus();
		MessageConsumer<JsonObject> consumer = eventBus.consumer("kafka-url-producer-adress");

		consumer.handler(message -> {
			String action = message.body().getString("action");
			
			
			String tn = map1.get("topicNr");
			
			if(tn.equals("0")) {
				map1.put("topicNr", "1");
			} else {
				map1.put("topicNr", "0");
			}

			switch (action) {
			case "send-url":
				sendUrl(message, tn);
				break;

			default:
				message.fail(1, "Unknown acton: " + message.body());
			}
		});
	}

	private void sendUrl(Message<JsonObject> message, String tn) {
		String urlAsString = (String) message.body().getValue("url");
		// KafkaProducerRecord.create(topic, key, value, partition)
		
		KafkaProducerRecord<String, String> record;
		
		
		
		if (tn.equals("0")) {
			record = KafkaProducerRecord.create("url-upstream-zero-topic", "url", urlAsString);
		} else {
			record = KafkaProducerRecord.create("url-upstream-one-topic", "url", urlAsString);
		}
		

		producer.send(record, done -> {

			if (done.succeeded()) {

				RecordMetadata recordMetadata = done.result();
				System.out.println("Message " + record.value() + " written on topic=" + recordMetadata.getTopic()
						+ ", partition=" + recordMetadata.getPartition() + ", offset=" + recordMetadata.getOffset());
				
				message.reply("Send Sucess");

			} else {
				System.out.println("Send Failed");
				message.fail(2, "Send Failed");
			}

		});
	}

}

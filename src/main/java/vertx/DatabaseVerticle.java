package vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.mongo.MongoAuth;
import io.vertx.ext.mongo.MongoClient;

public class DatabaseVerticle extends AbstractVerticle {
	
	MongoClient mongoClient;
	
	@Override
	public void start(Future<Void> startFuture) {
		
		//String connectionString = "mongodb://192.168.99.100:27017";
		String connectionString = "mongodb://localhost:27017";
		JsonObject mongoJson = new JsonObject()
				//.put("db_name", "canvas")
				.put("db_name", "crawler")
				.put("connection_string", connectionString);
		mongoClient = MongoClient.createShared(vertx, mongoJson);
		//Create collection with indexed value for email
		//mongoClient.createCollection("url", ar ->)
		
		//Create collection if not exist
		
		EventBus eventBus = vertx.eventBus();
		MessageConsumer<JsonObject> consumer = eventBus.consumer("db-adress"); 
		
		consumer.handler(message -> {
			String action = message.body().getString("action");
			
			switch (action) {
				case "add-data":
					addData(message);
					break;
				case "add-url":
					addUrl(message);
					break;
				case "get-diagrams":
					getDiagrams(message);
					break;
				
				default:
					message.fail(1, "Unknown acton: " + message.body());
			}
		});
		
		startFuture.complete();
	}
	
	private void addUrl(Message<JsonObject> message) {
		System.out.println("DB Verticle add url");
		String urlAsString = (String) message.body().getValue("url");
		JsonObject dataJson = new JsonObject().put("url", urlAsString).put("crawled", false);
		//System.out.println(dataJson.toString());
		
		mongoClient.save("url", dataJson, ar -> {
			if (ar.succeeded()) {
				String id = ar.result();
				
				System.out.println("DBV - Saved URL with id: " + id);
				sendUrl(urlAsString);
				message.reply(dataJson);
			} else {
				//System.out.println("DBV - URL not saved");
				message.fail(2, "insert failed: " + ar.cause().getMessage());	
			}
		});
	}
	
	private void addData(Message<JsonObject> message) {
		System.out.println("DB Verticle addData");
		String dataAsString = (String) message.body().getValue("data");
		//System.out.println(dataAsString);
		
		//JsonObject dataJson = message.body().getJsonObject("data");
		JsonObject dataJson = new JsonObject().put("data", dataAsString);
		//System.out.println(dataJson.toString());
		
		mongoClient.save("data", dataJson, ar -> {
			if (ar.succeeded()) {
				String id = ar.result();
				System.out.println("Saved data with id: " + id);
				message.reply(dataJson);
			} else {
				message.fail(2, "insert failed: " + ar.cause().getMessage());
			}
		});
	}
	
	
	@SuppressWarnings({ "unused", "deprecation" })
	private void sendUrl(String url) {
		JsonObject message = new JsonObject().put("action", "send-url").put("url", url);

		vertx.<JsonObject>eventBus().send("kafka-url-producer-adress", message, ar -> {

			if (ar.succeeded()) {
				System.out.println("DBV - URL sent to kafka url producer");
				//Skicka tillbaka till crawler att denna ska crawlas
			} else {
				System.out.println("DBV - URL NOT sent to kafka url producer");
			}
		});
	}
	
	private void getDiagrams(Message<JsonObject> message) {
		System.out.println("DB Verticle getDiagrams");
		//System.out.println(message.body().getValue("diagram"));
		
		JsonObject query = new JsonObject();
		
		mongoClient.find("diagrams", query, ar -> {
			if (ar.succeeded()) {
				JsonArray diagramJsonArray = new JsonArray();
				for (JsonObject diagramJson : ar.result()) {
					System.out.println(diagramJson.encodePrettily());
					diagramJsonArray.add(diagramJson);
					
				}
				System.out.println(" -------------------------- ");
				System.out.println(diagramJsonArray.encodePrettily());
				message.reply(diagramJsonArray);
			} else {
				message.fail(2, "get failed: " + ar.cause().getMessage());
			}
		});
	}
}

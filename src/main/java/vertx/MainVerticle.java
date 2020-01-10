package vertx;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class MainVerticle extends AbstractVerticle  {
	
	@Override
	public void start(Future<Void> startFuture) throws Exception {
		CompositeFuture.all(
			//deployVerticle(RestVerticle.class.getName()),
			deployVerticle(DatabaseVerticle.class.getName()),
			deployVerticle(KafkaDataConsumerVerticle.class.getName()),
			deployVerticle(KafkaUrlConsumerVerticle.class.getName()),
			deployVerticle(KafkaUrlProducerVerticle.class.getName())
			).setHandler(f -> {
				if(f.succeeded()) {
					startFuture.complete();
				} else {
					startFuture.fail(f.cause());
				}
			});
	}
	
	Future<Void> deployVerticle(String verticleName) {
		Future<Void> retVal = Future.future();
		vertx.deployVerticle(verticleName, event -> {
			if (event.succeeded()) {
				retVal.complete();
			} else {
				retVal.fail(event.cause());
			}
		});
		return retVal;
	}
	
	public static void main(String[] args) {
		
    	Vertx vertx = Vertx.vertx();
    	
    	vertx.deployVerticle(new MainVerticle());
	}
}

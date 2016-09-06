package smartthings.konsumer.example

import kafka.serializer.StringDecoder
import kafka.utils.VerifiableProperties
import smartthings.konsumer.KafkaListener
import smartthings.konsumer.ListenerConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import smartthings.konsumer.event.KonsumerEvent
import smartthings.konsumer.event.KonsumerEventListener
import smartthings.konsumer.filters.RetryMessageFilter

public class Main {

	private final static Logger log = LoggerFactory.getLogger(Main.class);

	private void run() throws Exception {
		log.info("Lets listen to Kafka!!!")
		ListenerConfig config = ListenerConfig.builder()
				.partitionThreads(1)
				.processingThreads(8)
				.processingQueueSize(10)
				.consumerGroup("konsumer-test")
				.topic("konsumer_test")
				.zookeeper("127.0.0.1:2181")
				.setProperty("auto.offset.reset", "smallest")
				.build();
		final KafkaListener<String, String, String> consumer = new KafkaListener(config,
			new StringDecoder(new VerifiableProperties()),
			new StringDecoder(new VerifiableProperties()));
		// call the blocking run method so the application doesn't exit and
		// stop the queue processing

		consumer.registerEventListener(new KonsumerEventListener() {
			@Override
			void eventNotification(KonsumerEvent event) {
				log.info('KonsumerEvent: {}', event.name())
			}
		})

		consumer.runAndBlock(new StringMessageProcessor(),
				new RetryMessageFilter(3)
		);
	}

	public static void main(String[] args) throws Exception {
		new Main().run();
	}
}

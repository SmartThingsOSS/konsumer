package smartthings.konsumer.stream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.filterchain.MessageFilterChain;

public class SingleMessageConsumer<K, V, R> implements Runnable {

	private final static Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

	private final KafkaStream<K, V> stream;
	private final MessageFilterChain<K, V, R> filterChain;
	private final CircuitBreaker circuitBreaker;

	public SingleMessageConsumer(KafkaStream<K, V> stream, MessageFilterChain<K, V, R> filterChain,
								 CircuitBreaker circuitBreaker) {
		this.stream = stream;
		this.filterChain = filterChain;
		this.circuitBreaker = circuitBreaker;
	}

	@Override
	public void run() {
		ConsumerIterator<K, V> it = stream.iterator();
		while (it.hasNext()) {
			circuitBreaker.blockIfOpen();
			MessageAndMetadata<K, V> messageAndMetadata = it.next();
			processMessage(messageAndMetadata);
		}
		log.warn("Shutting down listening thread");
	}

	private void processMessage(MessageAndMetadata<K, V> messageAndMetadata) {
		try {
			filterChain.handle(messageAndMetadata, circuitBreaker);
			return;
		} catch (Exception e) {
			log.error("Exception occurred during message processing", e);
		}
		log.warn("Shutting down listening thread");
	}
}

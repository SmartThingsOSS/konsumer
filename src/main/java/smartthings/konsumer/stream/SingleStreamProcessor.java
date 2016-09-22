package smartthings.konsumer.stream;

import kafka.consumer.KafkaStream;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.filterchain.MessageFilterChain;

public class SingleStreamProcessor<K, V, R> implements StreamProcessor<K, V, R> {

	@Override
	public Runnable buildConsumer(KafkaStream<K, V> stream, MessageFilterChain<K, V, R> filterChain,
								  CircuitBreaker circuitBreaker) {
		return new SingleMessageConsumer<>(stream, filterChain, circuitBreaker);
	}

	@Override
	public void shutdown() {
		// Nothing to do here. Shutting down the partition thread pool should be enough.
	}
}

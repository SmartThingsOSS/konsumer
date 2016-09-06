package smartthings.konsumer.filterchain;

import kafka.message.MessageAndMetadata;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;

public interface MessageContext<K, V, R> {

	R next(MessageAndMetadata<K, V> messageAndMetadata) throws Exception;

	CircuitBreaker getCircuitBreaker();

}

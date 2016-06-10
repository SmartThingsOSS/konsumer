package smartthings.konsumer.circuitbreaker;

import smartthings.konsumer.KafkaListener;

public interface CircuitBreaker {

	void init(CircuitBreakerListener listener);

	void blockIfOpen();

	boolean isOpen();

	void open(String sourceId);

	void conditionalClose(String sourceId);

	void destroy();

}

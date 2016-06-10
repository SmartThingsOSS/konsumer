package smartthings.konsumer.circuitbreaker;

public interface CircuitBreakerListener {

	void opened();

	void closed();

}

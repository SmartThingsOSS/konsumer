package smartthings.konsumer.circuitbreaker

import smartthings.konsumer.event.KonsumerEvent
import spock.lang.Specification


class SimpleCircuitBreakerSpec extends Specification {

	def 'should set the circuit breaker to the open state after calling open'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		CircuitBreakerListener listener = Mock()
		circuitBreaker.init(listener)

		when:
		circuitBreaker.open(this.getClass().toString())

		then:
		circuitBreaker.isOpen()
	}

	def 'should close the circuit breaker if no other peers has also opened it'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		CircuitBreakerListener listener = Mock()
		circuitBreaker.init(listener)
		circuitBreaker.open(this.getClass().toString())

		when:
		circuitBreaker.conditionalClose(this.getClass().toString())

		then:
		!circuitBreaker.isOpen()
	}

	def 'should not be able to close the circuit breaker if another peer has opened it'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		CircuitBreakerListener listener = Mock()
		circuitBreaker.init(listener)
		circuitBreaker.open('trigger')
		circuitBreaker.open('anotherTrigger')

		when:
		circuitBreaker.conditionalClose('trigger')

		then:
		circuitBreaker.isOpen()
	}

	def 'should close circuit after all sources have tried to close it'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		CircuitBreakerListener listener = Mock()
		circuitBreaker.init(listener)
		circuitBreaker.open('trigger')
		circuitBreaker.open('anotherTrigger')

		when:
		circuitBreaker.conditionalClose('trigger')
		circuitBreaker.conditionalClose('anotherTrigger')

		then:
		!circuitBreaker.isOpen()
	}

	def 'should notify listeners when the circuit breaker is opened'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		CircuitBreakerListener listener = Mock()
		circuitBreaker.init(listener)

		when:
		circuitBreaker.open(this.getClass().toString())

		then:
		1 * listener.opened()
		0 * _
	}

	def 'should notify listeners when the circuit breaker is closed'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		CircuitBreakerListener listener = Mock()
		circuitBreaker.init(listener)
		circuitBreaker.open(this.getClass().toString())

		when:
		circuitBreaker.conditionalClose(this.getClass().toString())

		then:
		1 * listener.closed()
		0 * _
	}

}

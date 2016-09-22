package smartthings.konsumer.stream

import kafka.consumer.ConsumerIterator
import kafka.consumer.KafkaStream
import kafka.message.MessageAndMetadata
import smartthings.konsumer.MessageProcessor
import smartthings.konsumer.circuitbreaker.CircuitBreaker
import smartthings.konsumer.filterchain.MessageFilterChain
import spock.lang.Specification

class SingleMessageConsumerSpec extends Specification {

	def 'should verify circuit breaker state before consuming a message'() {
		given:
		KafkaStream<byte[], byte[]> stream = Mock()
		ConsumerIterator<byte[], byte[]> streamIterator = Mock()
		CircuitBreaker circuitBreaker = Mock()
		MessageFilterChain filterChain = Mock()
		MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()
		int cnt = 0
		SingleMessageConsumer consumer = new SingleMessageConsumer(stream, filterChain, circuitBreaker)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext() >> { cnt += 1; return cnt == 1 }
		1 * circuitBreaker.blockIfOpen()
		1 * streamIterator.next() >> messageAndMetadata
		1 * filterChain.handle(messageAndMetadata, circuitBreaker)
		0 * _
	}

	def 'should process every message'() {
		given:
		KafkaStream<byte[], byte[]> stream = Mock()
		ConsumerIterator<byte[], byte[]> streamIterator = Mock()
		CircuitBreaker circuitBreaker = Mock()
		MessageFilterChain filterChain = Mock()
		MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()
		int cnt = 0
		int numMessages = 5
		SingleMessageConsumer consumer = new SingleMessageConsumer(stream, filterChain, circuitBreaker)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		(numMessages + 1) * streamIterator.hasNext() >> { cnt += 1; return cnt <= numMessages }
		numMessages * circuitBreaker.blockIfOpen()
		numMessages * streamIterator.next() >> messageAndMetadata
		numMessages * filterChain.handle(messageAndMetadata, circuitBreaker)
		0 * _
	}

	def 'should process messages with a decoder'() {
		given:
		KafkaStream<String, String> stream = Mock()
		ConsumerIterator<String, String> streamIterator = Mock()
		CircuitBreaker circuitBreaker = Mock()
		MessageFilterChain filterChain = Mock()
		MessageAndMetadata<String, String> messageAndMetadata = Mock()
		SingleMessageConsumer<String, String, Void> consumer = new SingleMessageConsumer<>(stream, filterChain,
			circuitBreaker)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext() >>> [true, false]
		1 * circuitBreaker.blockIfOpen()
		1 * streamIterator.next() >> messageAndMetadata
		1 * filterChain.handle(messageAndMetadata, circuitBreaker)
		0 * _
	}

}

package smartthings.konsumer.stream

import kafka.consumer.ConsumerIterator
import kafka.consumer.KafkaStream
import kafka.message.MessageAndMetadata
import smartthings.konsumer.circuitbreaker.CircuitBreaker
import smartthings.konsumer.filterchain.MessageFilterChain
import spock.lang.Specification

import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

class ThreadedMessageConsumerSpec extends Specification {

	KafkaStream<byte[], byte[]> stream = Mock()
	ConsumerIterator<byte[], byte[]> streamIterator = Mock()
	MessageFilterChain filterChain = Mock()
	CircuitBreaker circuitBreaker = Mock()
	MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()
	Semaphore semaphore = Mock()

	Executor currentTreadExecutor = new Executor() {
		@Override
		void execute(Runnable command) {
			command.run()
		}
	}

	def 'should verify circuit breaker state before consuming a message'() {
		given:
		def consumer = new ThreadedMessageConsumer(stream, currentTreadExecutor, semaphore, filterChain, circuitBreaker)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext()  >>> [true, false]
		1 * circuitBreaker.blockIfOpen()
		1 * streamIterator.next() >> messageAndMetadata
		1 * semaphore.acquire()
		1 * filterChain.handle(messageAndMetadata, circuitBreaker)
		1 * semaphore.release()
		0 * _
	}

	def 'should process every message'() {
		given:
		def consumer = new ThreadedMessageConsumer(stream, currentTreadExecutor, semaphore, filterChain, circuitBreaker)
		int cnt = 0
		int numMessages = 5

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		(numMessages + 1) * streamIterator.hasNext() >> { cnt += 1; return cnt <= numMessages }
		numMessages * circuitBreaker.blockIfOpen()
		numMessages * streamIterator.next() >> messageAndMetadata
		numMessages * semaphore.acquire()
		numMessages * filterChain.handle(messageAndMetadata, circuitBreaker)
		numMessages * semaphore.release()
		0 * _
	}

	def 'should process messages with a decoder'() {
		given:
		def consumer = new ThreadedMessageConsumer(stream, currentTreadExecutor, semaphore, filterChain, circuitBreaker)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext() >>> [true, false]
		1 * circuitBreaker.blockIfOpen()
		1 * streamIterator.next() >> messageAndMetadata
		1 * semaphore.acquire()
		1 * filterChain.handle(messageAndMetadata, circuitBreaker)
		1 * semaphore.release()
		0 * _
	}
}

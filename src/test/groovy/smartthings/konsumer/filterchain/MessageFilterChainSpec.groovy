package smartthings.konsumer.filterchain

import kafka.message.MessageAndMetadata
import smartthings.konsumer.MessageProcessor
import smartthings.konsumer.circuitbreaker.CircuitBreaker
import smartthings.konsumer.event.KonsumerEvent
import smartthings.konsumer.filters.BaseMessageFilter
import spock.lang.Specification


class MessageFilterChainSpec extends Specification {

	CircuitBreaker circuitBreaker = Mock()
	MessageProcessor<byte[], byte[], Void> messageProcessor = Mock()
	MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()

	def 'should call message processor directly if there are no filters'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Collections.emptyList())

		when:
		filterChain.handle(messageAndMetadata, circuitBreaker)

		then:
		1 * messageProcessor.processMessage(messageAndMetadata)
		0 * _
	}

	def 'should call every filter in the chain in order before calling the message processor'() {
		given:
		int counter = 0;
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Arrays.asList(
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				assert counter == 0;
				counter++
				ctx.next(messageAndMetadata)
			}
		},
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				assert counter == 1
				counter++
				ctx.next(messageAndMetadata)
			}
		}))

		when:
		filterChain.handle(messageAndMetadata, circuitBreaker)

		then:
		counter == 2
		1 * messageProcessor.processMessage(messageAndMetadata)
		0 * _
	}

	def 'should not call message processor if filter does not invoke the next filter in the chain'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Arrays.asList(
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				throw new RuntimeException()
			}
		}))

		when:
		filterChain.handle(messageAndMetadata, circuitBreaker)

		then:
		def e = thrown(RuntimeException)
		0 * _
	}

	def 'should allow filter to retry calls to filters and message processor'() {
		given:
		int numTries = 3
		int filterCounter = 0;
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Arrays.asList(
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				for (int i = 0; i < numTries; i++) {
					ctx.next(messageAndMetadata)
				}
			}
		},
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				filterCounter++
				ctx.next(messageAndMetadata)
			}
		}))

		when:
		filterChain.handle(messageAndMetadata, circuitBreaker)

		then:
		filterCounter == numTries
		numTries * messageProcessor.processMessage(messageAndMetadata)
		0 * _
	}

	def 'should allow filters to open circuit breaker'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Arrays.asList(
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				ctx.circuitBreaker.open('trigger')
			}
		}))

		when:
		filterChain.handle(messageAndMetadata, circuitBreaker)

		then:
		1 * circuitBreaker.open(_)
		0 * _
	}

	def 'should allow filters to close circuit breaker'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Arrays.asList(
		new BaseMessageFilter<byte[], byte[], Void>() {
			@Override
			Void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext<byte[], byte[], Void> ctx) throws Exception {
				ctx.circuitBreaker.conditionalClose('trigger')
			}
		}))

		when:
		filterChain.handle(messageAndMetadata, circuitBreaker)

		then:
		1 * circuitBreaker.conditionalClose(_)
		0 * _
	}

	def 'should invoke lifecycle methods on all filters in the chain'() {
		given:
		MessageFilter filter1 = Mock()
		MessageFilter filter2 = Mock()
		MessageFilterChain filterChain = new MessageFilterChain(messageProcessor, Arrays.asList(filter1, filter2))

		when:
		filterChain.getKonsumerEventListener().eventNotification(KonsumerEvent.STARTED)
		filterChain.getKonsumerEventListener().eventNotification(KonsumerEvent.SUSPENDED)
		filterChain.getKonsumerEventListener().eventNotification(KonsumerEvent.RESUMED)
		filterChain.getKonsumerEventListener().eventNotification(KonsumerEvent.STOPPED)

		then:
		1 * filter1.init()
		1 * filter1.suspended()
		1 * filter1.resumed()
		1 * filter1.destroy()
		1 * filter2.init()
		1 * filter2.suspended()
		1 * filter2.resumed()
		1 * filter2.destroy()
		0 * _
	}

}

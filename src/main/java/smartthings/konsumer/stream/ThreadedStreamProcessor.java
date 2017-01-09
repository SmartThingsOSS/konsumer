package smartthings.konsumer.stream;

import smartthings.konsumer.ListenerConfig;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.filterchain.MessageFilterChain;
import smartthings.konsumer.util.ThreadFactoryBuilder;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ThreadedStreamProcessor<K, V, R> implements StreamProcessor<K, V, R> {

	private final static Logger log = LoggerFactory.getLogger(ThreadedStreamProcessor.class);

	private final ExecutorService processingExecutor;
	private final ListenerConfig config;
	private final String topic;
	private final Semaphore taskSemaphore;

	public ThreadedStreamProcessor(ListenerConfig config) {
		this.config = config;
		processingExecutor = buildConsumerExecutor();
		topic = config.getTopic();
		taskSemaphore = new Semaphore(config.getProcessingThreads());
	}

	@Override
	public ThreadedMessageConsumer<K, V, R> buildConsumer(KafkaStream<K, V> stream, MessageFilterChain<K, V, R> filterChain,
												 CircuitBreaker circuitBreaker) {
		return new ThreadedMessageConsumer<>(stream, processingExecutor, taskSemaphore, filterChain, circuitBreaker);
	}

	private ExecutorService buildConsumerExecutor() {
		ThreadFactory messageThreadFactory = new ThreadFactoryBuilder()
				.setNameFormat("KafkaConsumer-" + config.getTopic() + "-%d")
				.setDaemon(config.useDaemonThreads())
				.build();
		return new ThreadPoolExecutor(
				config.getProcessingThreads(),
				config.getProcessingThreads(),
				0L,
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(config.getProcessingQueueSize()),
				messageThreadFactory
		);
	}

	@Override
	public void shutdown() {
		processingExecutor.shutdown();
		try {
			boolean completed = processingExecutor.awaitTermination(config.getShutdownAwaitSeconds(), TimeUnit.SECONDS);
			if (completed) {
				log.info("Shutdown processing consumers of topic {} all messages processed", topic);
			} else {
				log.warn("Shutdown processing consumers of topic {}. Some messages left unprocessed.", topic);
			}
		} catch (InterruptedException e) {
			log.error("Interrupted while waiting for shutdown of topic {}", topic, e);
		}
	}
}

package smartthings.konsumer;

import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.circuitbreaker.CircuitBreakerListener;
import smartthings.konsumer.circuitbreaker.SimpleCircuitBreaker;
import smartthings.konsumer.event.KonsumerEvent;
import smartthings.konsumer.event.KonsumerEventListener;
import smartthings.konsumer.filterchain.MessageFilter;
import smartthings.konsumer.filterchain.MessageFilterChain;
import smartthings.konsumer.stream.StreamProcessor;
import smartthings.konsumer.stream.StreamProcessorFactory;
import smartthings.konsumer.util.QuietCallable;
import smartthings.konsumer.util.RunUtil;
import smartthings.konsumer.util.ThreadFactoryBuilder;
import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
 * Main entry point for the konsumer library. Provides methods for configuring the konsumer and start
 * consuming messages from Kafka.
 *
 * @param <K> The type of the key used by the Kafka topic
 * @param <V> The type of the value used by Kafka topic
 * @param <R> The return value from processing a Kafka message
 */
public class KafkaListener<K, V, R> {
	private final static Logger log = LoggerFactory.getLogger(KafkaListener.class);

	private final ConsumerConnector consumer;
	private final ExecutorService partitionExecutor;
	private final StreamProcessor<K, V, R> streamProcessor;
	private final String topic;
	private final ListenerConfig config;
	private final Decoder<K> keyDecoder;
	private final Decoder<V> valueDecoder;
	private final CircuitBreaker circuitBreaker;
	private final List<KonsumerEventListener> listeners = new CopyOnWriteArrayList<>();

	/**
	 * Returns an instance of the KafkaListener that uses the default byte arrays types for both the
	 * key and the value. It also sets up the KafkaListener to not expect any return value.
	 *
	 * @param config Configuration parameters for the konsumer.
	 * @return
	 */
	public static KafkaListener<byte[], byte[], Void> getDefaultKafkaListener(ListenerConfig config) {
		Decoder<byte[]> defaultDecoder = new DefaultDecoder(new VerifiableProperties());
		return new KafkaListener<>(config, defaultDecoder, defaultDecoder);
	}

	/**
	 * Creates a new instance of KafkaListener configured as specified in the config and using the decoders
	 * provided.
	 *
	 * @param config Configuration parameters for the konsumer.
	 * @param keyDecoder The decoder for the kafka keys
	 * @param valueDecoder The decoder for the kafka values.
	 */
	public KafkaListener(ListenerConfig config, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
		this.config = config;
		this.keyDecoder = keyDecoder;
		this.valueDecoder = valueDecoder;
		// Build custom executor so we control the factory and backing queue
		// and get better thread names for logging
		partitionExecutor = buildPartitionExecutor();

		consumer = Consumer.createJavaConsumerConnector(config.getConsumerConfig());
		topic = config.getTopic();
		streamProcessor = new StreamProcessorFactory<K, V, R>(config).getProcessor();
		circuitBreaker = new SimpleCircuitBreaker();
	}

	private ExecutorService buildPartitionExecutor() {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setNameFormat("KafkaPartition-" + config.getTopic() + "-%d")
				.setDaemon(config.useDaemonThreads())
				.build();
		return Executors.newFixedThreadPool(config.getPartitionThreads(), threadFactory);
	}

	/**
	 * Shuts down the consumer. This will stop consumption of all messages from Kafka and notify any konsumer
	 * event listeners.
	 */
	public void shutdown() {
		consumer.shutdown();
		partitionExecutor.shutdown();
		try {
			boolean completed = partitionExecutor.awaitTermination(config.getShutdownAwaitSeconds(), TimeUnit.SECONDS);
			if (completed) {
				log.info("Shutdown partition consumers of topic {} all messages processed", topic);
			} else {
				log.warn("Shutdown partition consumers of topic {}. Some messages left unprocessed.", topic);
			}
		} catch (InterruptedException e) {
			log.error("Interrupted while waiting for shutdown of topic {}", topic, e);
		}
		streamProcessor.shutdown();
		notifyEventListeners(KonsumerEvent.STOPPED);
	}

	/**
	 * @see KafkaListener#run(MessageProcessor, List)
	 *
	 * @param processor An implementation of the {@link MessageProcessor} interface that will process kafka messages.
	 * @param filters Zero or more objects implementing the {@link MessageFilter} interface.
	 */
	@SafeVarargs
	public final void run(MessageProcessor<K, V, R> processor, MessageFilter<K, V, R>... filters) {
		run(processor, Arrays.asList(filters));
	}

	/**
	 * Starts the konsumer and kafka messages will be passed through the message filters before finally
	 * bing processed by the message processor.
	 *
	 * @param processor An implementation of the {@link MessageProcessor} interface that will process kafka messages.
	 * @param filters A list of objects implementing the {@link MessageFilter} interface.
	 */
	public final void run(MessageProcessor<K, V, R> processor, List<MessageFilter<K, V, R>> filters) {
		MessageFilterChain<K, V, R> filterChain = new MessageFilterChain<>(processor, filters);
		registerEventListener(filterChain.getKonsumerEventListener());
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, config.getPartitionThreads());
		Map<String, List<KafkaStream<K, V>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
		List<KafkaStream<K, V>> streams = consumerMap.get(topic);
		circuitBreaker.init(new CircuitBreakerListener() {
			@Override
			public void opened() {
				notifyEventListeners(KonsumerEvent.SUSPENDED);
			}

			@Override
			public void closed() {
				notifyEventListeners(KonsumerEvent.RESUMED);
			}
		});
		notifyEventListeners(KonsumerEvent.STARTED);

		log.info("Listening to kafka with {} partition threads", config.getPartitionThreads());
		for (KafkaStream<K, V> stream : streams) {
			try {
				partitionExecutor.submit(streamProcessor.buildConsumer(stream, filterChain, circuitBreaker));
			} catch (RejectedExecutionException e) {
				log.error("Error submitting job to partition executor");
				throw e;
			}
		}
	}

	private void notifyEventListeners(KonsumerEvent event) {
		for (KonsumerEventListener listener : listeners) {
			listener.eventNotification(event);
		}
	}

	/**
	 * Register an listener for lifecycle events for the konsumer
	 *
	 * @param listener A listener for konsumer events implementing the {@link KonsumerEventListener} interface.
	 */
	public void registerEventListener(KonsumerEventListener listener) {
		listeners.add(listener);
	}

	/**
	 * Suspend message processing from Kafka. This will open the circuit breaker and notify any listeners.
	 */
	public void suspend() {
		circuitBreaker.open(this.getClass().getCanonicalName());
	}

	/**
	 * Checks if the circuit breaker is currently open.
	 *
	 * @return boolean value that will be true if the circuit breaker is open and false otherwise.
	 */
	public boolean isSuspended() {
		return circuitBreaker.isOpen();
	}

	/**
	 * Closes the circuit breaker. Note, if the circuit breaker was opened by another object, calling resume here
	 * will not close the circuit breaker.
	 */
	public void resume() {
		circuitBreaker.conditionalClose(this.getClass().getCanonicalName());
	}

	/**
	 * Run and then block the calling thread until shutdown. In place to make it easy to
	 * consume in a main method and still exit cleanly.
	 * @param processor The handler that will be handle all messages consumed.
	 * @param filters An ordered list of the filters that will be applied to all messages consumed before they
	 *                are passed to the processor.
	 */
	public void runAndBlock(MessageProcessor<K, V, R> processor, MessageFilter<K, V, R>... filters) {
		run(processor, filters);
		RunUtil.blockForShutdown(new QuietCallable() {
			@Override
			public void call() {
				shutdown();
			}
		});
	}
}

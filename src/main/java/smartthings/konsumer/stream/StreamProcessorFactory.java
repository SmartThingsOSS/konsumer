package smartthings.konsumer.stream;

import smartthings.konsumer.ListenerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamProcessorFactory<K, V, R> {
	private final static Logger log = LoggerFactory.getLogger(StreamProcessorFactory.class);
	private final ListenerConfig config;

	public StreamProcessorFactory(ListenerConfig config) {
		this.config = config;
	}

	public StreamProcessor<K, V, R> getProcessor() {
		if (config.getPartitionThreads() >= config.getProcessingThreads()) {
			log.info("Building a single threaded stream processor");
			return new SingleStreamProcessor<>();
		} else {
			log.info("Building a threaded stream processor with {} threads", config.getProcessingThreads());
			return new ThreadedStreamProcessor<>(config);
		}
	}

}

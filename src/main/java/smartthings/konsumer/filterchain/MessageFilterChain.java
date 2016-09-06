package smartthings.konsumer.filterchain;

import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.MessageProcessor;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.event.KonsumerEvent;
import smartthings.konsumer.event.KonsumerEventListener;

import java.util.*;

public class MessageFilterChain<K, V, R> {
	private final static Logger log = LoggerFactory.getLogger(MessageFilterChain.class);

	private final MessageProcessor<K, V, R> processor;
	private final List<MessageFilter<K, V, R>> filters;

	public MessageFilterChain(final MessageProcessor<K, V, R> processor, final List<MessageFilter<K, V, R>> messageFilters) {
		this.processor = processor;
		this.filters = Collections.unmodifiableList(new ArrayList<>(messageFilters)); //make defensive copy
	}

	public R handle(MessageAndMetadata<K, V> originalMessageAndMetadata,
					   final CircuitBreaker circuitBreaker) throws Exception {

		MessageContext<K, V, R> context = new MessageContext<K, V, R>() {
			private int counter = 0;

			@Override
			public R next(MessageAndMetadata<K, V> messageAndMetadata) throws Exception {
				if (filters.size() > counter) {
					MessageFilter<K, V, R> filter = filters.get(counter);
					log.debug("Calling filterchain # {} - {}", counter, filter.getClass().toString());
					counter++;
					try {
						return filter.handleMessage(messageAndMetadata, this);
					} finally {
						counter--;
					}
				} else {
					log.debug("Calling processor # {} - {}", counter, processor.getClass().toString());
					return processor.processMessage(messageAndMetadata);
				}
			}

			@Override
			public CircuitBreaker getCircuitBreaker() {
				return circuitBreaker;
			}
		};

		return context.next(originalMessageAndMetadata);

	}

	private void invokeFilterLifecycleCallbacks(KonsumerEvent event) {
		for (MessageFilter filter : filters) {
			if (event == KonsumerEvent.STARTED) {
				filter.init();
			} else if (event == KonsumerEvent.STOPPED) {
				filter.destroy();
			} else if (event == KonsumerEvent.SUSPENDED) {
				filter.suspended();
			} else if (event == KonsumerEvent.RESUMED) {
				filter.resumed();
			}
		}
	}

	public KonsumerEventListener getKonsumerEventListener() {
		return new KonsumerEventListener() {
			@Override
			public void eventNotification(KonsumerEvent event) {
				invokeFilterLifecycleCallbacks(event);
			}
		};
	}

}

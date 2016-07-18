package smartthings.konsumer.circuitbreaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

@ThreadSafe
public class SimpleCircuitBreaker implements CircuitBreaker {
	private final static Logger log = LoggerFactory.getLogger(SimpleCircuitBreaker.class);

	private final Object mutex = new Object();

	@GuardedBy("mutex")
	private boolean open = false;
	@GuardedBy("mutex")
	private final Set<String> sources = new HashSet<>();

	private CircuitBreakerListener listener;

	@Override
	public void init(CircuitBreakerListener listener) {
		synchronized (mutex) {
			this.listener = listener;
		}
	}

	@Override
	public void destroy() {
		//do nothing
	}

	@Override
	public void blockIfOpen() {
		synchronized (mutex) {
			while (open) {
				try {
					log.debug("SimpleCircuitBreaker.blockIfOpen - calling wait - {}", Thread.currentThread().getName());
					mutex.wait();
				} catch (InterruptedException e) {
					//Intentionally left blank
				}
			}
		}
	}

	@Override
	public boolean isOpen() {
		synchronized (mutex) {
			return open;
		}
	}

	@Override
	public void open(String sourceId) {
		synchronized (mutex) {
			open = true;
			if (sources.isEmpty()) {
				log.info("CircuitBreaker opened by {}", sourceId);
				sources.add(sourceId);
				listener.opened();
			} else if (sources.contains(sourceId)) {
				log.info("CircuitBreaker has already been opened by {}", sourceId);
			} else {
				log.info("CircuitBreaker already open when open requested by {}", sourceId);
				sources.add(sourceId);
			}
		}
	}

	@Override
	public void conditionalClose(String sourceId) {
		synchronized (mutex) {
			if (sources.size() == 1 && sources.contains(sourceId)) {
				open = false;
				sources.remove(sourceId);
				listener.closed();
				log.info("conditionalClose - Closing - {}", Thread.currentThread().getName());
				mutex.notifyAll();
			} else {
				sources.remove(sourceId);
			}
		}
	}
}

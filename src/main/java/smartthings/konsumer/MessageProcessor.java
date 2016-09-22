package smartthings.konsumer;

import kafka.message.MessageAndMetadata;

public interface MessageProcessor<K, V, R> {
	R processMessage(MessageAndMetadata<K, V> message) throws Exception;
}

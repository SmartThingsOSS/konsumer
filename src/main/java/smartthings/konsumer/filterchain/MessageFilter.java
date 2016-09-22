package smartthings.konsumer.filterchain;

import kafka.message.MessageAndMetadata;

public interface MessageFilter<K, V, R> {

	void init();

	void suspended();

	void resumed();

	void destroy();

	R handleMessage(MessageAndMetadata<K, V> messageAndMetadata, MessageContext<K, V, R> ctx) throws Exception;

}

package smartthings.konsumer.filterchain;

import kafka.message.MessageAndMetadata;

public interface MessageFilter {

	void init();

	void suspended();

	void resumed();

	void destroy();

	void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception;

}

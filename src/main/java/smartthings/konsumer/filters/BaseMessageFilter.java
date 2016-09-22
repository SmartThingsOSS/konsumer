package smartthings.konsumer.filters;

import kafka.message.MessageAndMetadata;
import smartthings.konsumer.filterchain.MessageContext;
import smartthings.konsumer.filterchain.MessageFilter;

public abstract class BaseMessageFilter<K, V, R> implements MessageFilter<K, V, R> {

	@Override
	public void init() {}

	@Override
	public void suspended() {}

	@Override
	public void resumed() {}

	@Override
	public void destroy() {}

	@Override
	public abstract R handleMessage(MessageAndMetadata<K, V> message, MessageContext<K, V, R> ctx) throws Exception;
}

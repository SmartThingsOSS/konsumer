package smartthings.konsumer.filters;

import kafka.message.MessageAndMetadata;
import smartthings.konsumer.filterchain.MessageContext;
import smartthings.konsumer.filterchain.MessageFilter;

public abstract class BaseMessageFilter implements MessageFilter {

	@Override
	public void init() {}

	@Override
	public void suspended() {}

	@Override
	public void resumed() {}

	@Override
	public void destroy() {}

	@Override
	public abstract void handleMessage(MessageAndMetadata<byte[], byte[]> message, MessageContext ctx) throws Exception;
}

package smartthings.konsumer.example;


import kafka.message.MessageAndMetadata;
import smartthings.konsumer.MessageProcessor;

public class StringMessageProcessor implements MessageProcessor<String, String, String> {

	@Override
	public String processMessage(MessageAndMetadata<String, String> message) throws Exception {
		System.out.println(String.format("Key %s, Message %s", message.key(), message.message()));
		return "ReturnMessage";
	}
}

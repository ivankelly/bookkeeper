package com.yonyou.msgbus.example.pubsub;

import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MsgBusClient;
import com.yonyou.msgbus.client.PubSubClient;

public class AsyncPubSubConsumer {

	/**
	 * @param args
	 */		
	

	public static void main(String[] args) throws Exception {
		AsyncPubSubConsumer consumer= new AsyncPubSubConsumer();
		consumer.asyncConsume(args);
		
	}
	
	public void asyncConsume(String[] args) throws MalformedURLException, ConfigurationException, ClientNotSubscribedException, CouldNotConnectException, ClientAlreadySubscribedException, ServiceDownException, InvalidSubscriberIdException, AlreadyStartDeliveryException, InterruptedException {
		if (args.length < 3) {
			System.out.println("Parameters: topic subid numToRecv");
			return;
		}
		
		final String myTopic= args[0];
		final String mySubid= args[1];
		final int numMessages = Integer.parseInt(args[2]);	
		final AtomicInteger numReceived = new AtomicInteger(0);
		final CountDownLatch receiveLatch = new CountDownLatch(1);

		long start = System.currentTimeMillis();
		String path = "F:/Java Projects2/msgbus/hw_client.conf";	
		final MsgBusClient client = new MsgBusClient(path);	
		
		final PubSubClient pubSubClient=client.getPubSubClient();
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
				.setMessageWindowSize(100).build();
		pubSubClient.subscribe(myTopic, mySubid, options);		
		System.out.println(numMessages);
		pubSubClient.startDelivery(myTopic, mySubid, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				System.out.println(msg.getBody().toStringUtf8());
				//System.out.println(msg.getMsgId().getLocalComponent());
				

				if (numMessages == numReceived.incrementAndGet()) {					
					receiveLatch.countDown();
				}		
				
				try {					
					pubSubClient.consume(myTopic, mySubid, msg.getMsgId());
				} catch (ClientNotSubscribedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
		});
		
		assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(3000, TimeUnit.SECONDS));
		pubSubClient.stopDelivery(myTopic, mySubid);
		pubSubClient.closeSubscription(myTopic, mySubid);	
	
		
		long end = System.currentTimeMillis();
		System.out.println("Received " +numMessages+" messaged in "+ (end - start) + " ms.");
	}

}

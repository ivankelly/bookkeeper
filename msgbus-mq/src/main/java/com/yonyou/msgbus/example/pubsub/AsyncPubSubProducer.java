package com.yonyou.msgbus.example.pubsub;

import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yonyou.msgbus.client.MsgBusClient;
import com.yonyou.msgbus.client.PubSubClient;
public class AsyncPubSubProducer {
	private PubSubClient client;
	public static Logger logger = LoggerFactory.getLogger(AsyncPubSubProducer.class);

	public AsyncPubSubProducer(String path) throws MalformedURLException, ConfigurationException {
		final MsgBusClient msgBusClient = new MsgBusClient(path);
		client=msgBusClient.getPubSubClient();
	}

	/**
	 * @param args
	 */
	
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
    		System.out.println("Parameters: topic msgLen num");
			return;
		}
		AsyncPubSubProducer p=new AsyncPubSubProducer("F:/Java Projects3/hedwig/hw_client.conf");
		p.asyncSendWithResponse(args);		
	}
	
	
	public void asyncSendWithResponse(String args[]) throws Exception {		
		if (args.length < 3) {
			System.out.println("Parameters: topic msgLen numToPub");
			return;
		}
		
		String topic= args[0];

		int length = Integer.parseInt(args[1]);
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < length; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();

		final int numMessages = Integer.parseInt(args[2]);		

		final AtomicInteger numPublished = new AtomicInteger(0);
		final CountDownLatch publishLatch = new CountDownLatch(1);
		//final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

		new AtomicInteger(0);
		new CountDownLatch(1);
		new HashMap<String, MessageSeqId>();

		long start = System.currentTimeMillis();
		
		System.out.println("Start to asynsPublish!");
		//first publish a message synchronously, to establish a connection.	
		client.publish(topic, prefix+0);		
		
		if (numMessages == numPublished.incrementAndGet()) {
			publishLatch.countDown();
		}
		
		//then pub other messages asynchronously
		for (int i = 1; i < numMessages; i++) {			

			final String str = prefix+i;
			
			client.asyncPublishWithResponse(topic, str,
					new Callback<PublishResponse>() {
						@Override
						public void operationFinished(Object ctx,
								PublishResponse response) {
							// map, same message content results wrong
							//publishedMsgs.put(str, response.getPublishedMsgId());
							if (numMessages == numPublished.incrementAndGet()) {
								publishLatch.countDown();
							}
						}

						@Override
						public void operationFailed(Object ctx,
								final PubSubException exception) {
							
						}
					}, null);			
		}
		long end = System.currentTimeMillis();

		// wait the work to finish
		assertTrue("Timed out waiting on callback for publish requests.",
				publishLatch.await(300, TimeUnit.SECONDS));
		
		System.out.println("AsyncPublished " + numMessages + " messages in " + (end - start) + " ms.");
	}

}

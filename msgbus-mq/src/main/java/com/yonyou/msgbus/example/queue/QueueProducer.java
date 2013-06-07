package com.yonyou.msgbus.example.queue;

import static org.junit.Assert.assertTrue;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;
public class QueueProducer {
	final MsgBusClient msgBusClient;
	private MessageQueueClient client;
	public static Logger logger = LoggerFactory.getLogger(QueueProducer.class);

	public QueueProducer(String path) throws MalformedURLException, ConfigurationException {
		msgBusClient = new MsgBusClient(path);
		client=msgBusClient.getMessageQueueClient();
		
	}

	/**
	 * @param args
	 */
	
	public static void main(String[] args) throws Exception {
		String[] params={"test","50","100"};
		if (params.length < 3) {
    		System.out.println("Parameters: queueName msgLen num");
			return;
		}		
		QueueProducer p=new QueueProducer("F:/Java Projects2/msgbus/hw_client.conf");
		p.asyncSendWithResponse(params);		
	}

	public void send(String[] args) throws CouldNotConnectException, ServiceDownException, InterruptedException {
		long start= System.currentTimeMillis();
		String queueName = args[0];		
		int num = Integer.parseInt(args[2]);
		
		int msgLen = Integer.parseInt(args[1]);		
		StringBuffer sb = new StringBuffer("");
		for (int i = 0; i < msgLen; i++) {
			sb.append("a");
		}
		final String prefix = sb.toString();		
	
		client.createQueue(queueName);
		for(int i=0; i<num; i++){
			client.publish(queueName, prefix+i);
			
		}
		long end= System.currentTimeMillis();
		System.out.println("Sent "+ num +" messages in "+(end-start)+" ms");
		msgBusClient.close();
	}
	
	public void asyncSendWithResponse(String args[]) throws Exception {		
		if (args.length < 3) {
			System.out.println("Parameters: topic msgLen num");
			return;
		}
		
		System.out.println("Version: 2012-11-16.");
		String queueName= args[0];

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
		client.createQueue(queueName);
		client.publish(queueName, prefix+0);
		
		//publishedMsgs.put(prefix+0, res.getPublishedMsgId());
		if (numMessages == numPublished.incrementAndGet()) {
			publishLatch.countDown();
		}
		
		for (int i = 1; i < numMessages; i++) {			

			final String str = prefix+i;
			
			client.asyncPublishWithResponse(queueName, str,
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
		msgBusClient.close();
	}

}

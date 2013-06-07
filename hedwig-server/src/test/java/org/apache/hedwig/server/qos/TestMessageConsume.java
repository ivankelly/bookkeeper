package org.apache.hedwig.server.qos;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase1;
import org.apache.hedwig.util.Callback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class TestMessageConsume extends HedwigHubTestBase1 {
    MsgBusClient msgBusClient;
    MessageQueueClient queueClient;
    URL path = this.getClass().getResource("/hw_client.conf");

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty("build.test.dir", "F:\\test1");
        super.setUp();
        msgBusClient = new MsgBusClient(path);
        queueClient = msgBusClient.getMessageQueueClient();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testWithAllConsume() throws Exception {
        final String queueName = "testconsuming";
        String prefix = "message";
        final long numMessages = 1000;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> receivedMsgs = new HashMap<String, MessageSeqId>();

        queueClient.createQueue(queueName);
        // publish messages!
        for (int i = 1; i <= numMessages; i++) {
            final String str = prefix + i;

            queueClient.asyncPublishWithResponse(queueName, str, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    // map, same message content results wrong
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);

        }

        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(1000).build();

        queueClient.createQueue(queueName);
        queueClient.startDelivery(queueName, new MessageHandler() {
            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs.put(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                try {
                    queueClient.consumeMessage(queueName, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                callback.operationFinished(context, null);
            }
        }, options);

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publishe responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " received messages.", numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " received messages in map.", numMessages,
                receivedMsgs.size());

        long messageCount = queueClient.queryMessageCount(queueName);
        assertEquals("There are still messages in server cache!!", messageCount, 0);
        queueClient.stopDelivery(queueName);
        queueClient.closeSubscription(queueName);

    }

    @Test
    public void testWithSomeConsume() throws Exception {

        final String queueName = "testWithSomeConsume";
        String prefix = "message";
        final long numMessages = 1000;
        long numConsume = 500;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> receivedMsgs = new HashMap<String, MessageSeqId>();

        queueClient.createQueue(queueName);
        // publish messages!
        for (int i = 1; i <= numMessages; i++) {
            final String str = prefix + i;

            queueClient.asyncPublishWithResponse(queueName, str, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    // map, same message content results wrong
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);

        }

        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(1000).build();

        queueClient.createQueue(queueName);
        queueClient.startDelivery(queueName, new MessageHandler() {
            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs.put(str, msg.getMsgId());
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                    logger.info("receiving work finished..........................");
                }
                callback.operationFinished(context, null);
            }
        }, options);

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.", numMessages, numPublished.get());
        assertEquals("Should be expected " + numMessages + " publishe responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " received messages.", numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " received messages in map.", numMessages,
                receivedMsgs.size());

        for (int i = 1; i <= numConsume; i++) {
            try {
                queueClient.consumeMessage(queueName, receivedMsgs.get(prefix + i));
            } catch (ClientNotSubscribedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        logger.info("consuming work finished..........................");

        long messageCount = queueClient.queryMessageCount(queueName);
        assertEquals("There are still messages in server cache!!", messageCount, numMessages - numConsume);
        queueClient.stopDelivery(queueName);
        queueClient.closeSubscription(queueName);

    }

}

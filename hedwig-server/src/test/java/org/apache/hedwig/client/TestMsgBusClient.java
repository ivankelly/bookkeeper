package org.apache.hedwig.client;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
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
import org.apache.hedwig.util.ConcurrencyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class TestMsgBusClient extends HedwigHubTestBase1 {

    MsgBusClient msgBusClient;
    MessageQueueClient msgClient;
    URL path = this.getClass().getResource("/hw_client.conf");

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<Boolean> deliveryQueue = new SynchronousQueue<Boolean>();

    class TestCallback implements Callback<Void> {

        @Override
        public void operationFinished(Object ctx, Void resultOfOperation) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    if (logger.isDebugEnabled())
                        logger.debug("Operation finished!");
                    ConcurrencyUtils.put(queue, true);
                }
            }).start();
        }

        @Override
        public void operationFailed(Object ctx, final PubSubException exception) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    logger.error("Operation failed!", exception);
                    ConcurrencyUtils.put(queue, false);
                }
            }).start();
        }
    }

    // Test implementation of subscriber's message handler.
    class TestMessageHandler implements MessageHandler {

        private final SynchronousQueue<Boolean> deliveryQueue;

        public TestMessageHandler() {
            this.deliveryQueue = TestMsgBusClient.this.deliveryQueue;
        }

        //
        // public TestMessageHandler(SynchronousQueue<Boolean> consumeQueue) {
        // this.consumeQueue = consumeQueue;
        // }

        public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                Object context) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    if (logger.isDebugEnabled())
                        logger.debug("Consume operation finished successfully!");
                    ConcurrencyUtils.put(TestMessageHandler.this.deliveryQueue, true);
                }
            }).start();
            callback.operationFinished(context, null);
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        System.setProperty("build.test.dir", "F:\\test1");
        super.setUp();
        msgBusClient = new MsgBusClient(path);
        msgClient = msgBusClient.getMessageQueueClient();
    }

    @Override
    @After
    public void tearDown() throws Exception {

        super.tearDown();
    }

    @Test(timeout = 6000)
    public void testCreateQueue() throws Exception {
        final String queueName = "testCreateQueue";
        boolean createQueueSuccess = true;
        try {
            msgClient.createQueue(queueName);
        } catch (Exception e) {
            createQueueSuccess = false;
        }
        assertTrue("queue isn't created!", createQueueSuccess);
    }

    @Test(timeout = 60000)
    public void testDeleteQueue() throws Exception {
        final String queueName = "testDeleteQueue";
        boolean deleteQueueSuccess = true;
        try {
            msgClient.createQueue(queueName);
            msgClient.deleteQueue(queueName);
        } catch (Exception e) {
            deleteQueueSuccess = false;
        }
        assertTrue("queue isn't deleted", deleteQueueSuccess);
    }

    @Test(timeout = 60000)
    public void testSyncPublish() throws Exception {
        final String queueName = "testAsynPublish";
        boolean publishSuccess = true;
        try {
            msgClient.createQueue(queueName);
            msgClient.publish(queueName, "SyncPublishMessage");
        } catch (Exception e) {
            publishSuccess = false;
        }
        assertTrue("SyncPublish failed!", publishSuccess);
    }

    @Test(timeout = 60000)
    public void testAsyncPublish() throws Exception {
        final String queueName = "testAsynPublish";
        msgClient.createQueue(queueName);
        msgClient.asyncPublish(queueName, "AsyncPublishMessage", new TestCallback(), null);
        assertTrue("AsyncPublish failed!", queue.take());
    }

    @Test(timeout = 60000)
    public void testAsyncPublishWithResponse() throws Exception {

        final String queueName = "testAsynPublishWithResponse";
        final String prefix = "AsyncMessage-";
        final int numMessage = 30;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final Map<String, MessageSeqId> receivedMsgs = new HashMap<String, MessageSeqId>();

        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(100).build();

        msgClient.createQueue(queueName);
        msgClient.startDelivery(queueName, new MessageHandler() {
            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {

                String str = msg.getBody().toStringUtf8();
                receivedMsgs.put(str, msg.getMsgId());
                if (numMessage == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                try {
                    msgClient.consumeMessage(queueName, msg.getMsgId());
                } catch (ClientNotSubscribedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                callback.operationFinished(context, null);
            }
        }, options);

        // Start to asynsPublish!
        for (int i = 0; i < numMessage; i++) {

            final String str = prefix + i;

            msgClient.asyncPublishWithResponse(queueName, str, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessage == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);

        }

        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessage + " publishes.", numMessage, numPublished.get());
        assertEquals("Should be expected " + numMessage + " publishe responses.", numMessage, publishedMsgs.size());
        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessage + " messages.", numMessage, numReceived.get());
        assertEquals("Should be expected " + numMessage + " messages in map.", numMessage, receivedMsgs.size());

        for (int i = 0; i < numMessage; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str, pubId.equals(revId));
        }

        msgClient.stopDelivery(queueName);
        msgClient.closeSubscription(queueName);
    }

    @Test(timeout = 60000)
    public void testStartDelivery() throws Exception {
        final String queueName = "testStartDelivery";
        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(100).build();

        msgClient.createQueue(queueName);
        // Now publish some messages for the topic to be consumed by the
        // subscriber.
        msgClient.publish(queueName, "Message#testStartDelivery");
        // Start delivery for the subscriber
        msgClient.startDelivery(queueName, new TestMessageHandler(), options);
        assertTrue(deliveryQueue.take());

        msgClient.stopDelivery(queueName);
        msgClient.closeSubscription(queueName);
    }

    @Test(timeout = 60000)
    public void testConsumeMessage() throws Exception {
        final String queueName = "testStartDelivery";
        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(100).build();

        msgClient.createQueue(queueName);
        // Now publish some messages for the topic to be consumed by the
        // subscriber.
        msgClient.publish(queueName, "Message#testStartDelivery");
        // Start delivery for the subscriber
        msgClient.startDelivery(queueName, new MessageHandler() {
            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                try {
                    msgClient.consumeMessage(queueName, msg.getMsgId());
                    // if (logger.isDebugEnabled())
                    // logger.debug("Consume operation finished successfully!");
                    ConcurrencyUtils.put(consumeQueue, true);
                } catch (ClientNotSubscribedException e) {
                    logger.info("consume operation failed!");
                    ConcurrencyUtils.put(consumeQueue, false);
                }
                callback.operationFinished(context, null);
            }
        }, options);
        assertTrue(consumeQueue.take());

        msgClient.stopDelivery(queueName);
        msgClient.closeSubscription(queueName);
    }

    @Test(timeout = 60000)
    public void testStopDelivery() throws Exception {
        boolean stopDeliverySuccess = true;
        final String queueName = "testStopDelivery";
        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(100).build();
        msgClient.createQueue(queueName);
        msgClient.publish(queueName, "Message#testStopDelivery");
        msgClient.startDelivery(queueName, new TestMessageHandler(), options);
        assert (consumeQueue.take());
        try {
            msgClient.stopDelivery(queueName);
        } catch (Exception e) {
            stopDeliverySuccess = false;
        }
        assertTrue("fail to stop delivery!", stopDeliverySuccess);
        msgClient.closeSubscription(queueName);
    }

    @Test(timeout = 60000)
    public void testCloseSubscription() throws Exception {
        boolean closeSubscriptionSuccess = true;
        final String queueName = "testCloseSubscription";
        // subscribe messages
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(100).build();
        msgClient.createQueue(queueName);
        msgClient.publish(queueName, "Message#testCloseSubscription");
        msgClient.startDelivery(queueName, new TestMessageHandler(), options);
        assert (consumeQueue.take());
        msgClient.stopDelivery(queueName);
        try {
            msgClient.closeSubscription(queueName);
        } catch (Exception e) {
            closeSubscriptionSuccess = false;
        }
        assertTrue("fail to close Subscription!", closeSubscriptionSuccess);
    }

    @Test(timeout = 60000)
    public void testQueryMessage() throws Exception {
        int numMessage = 10;
        String prefix = "QueryMessage#";
        final String queueName = "testQueryMessage";
        msgClient.createQueue(queueName);
        for (int i = 0; i < numMessage; i++) {
            msgClient.publish(queueName, prefix + i);
        }
        assertEquals("should be expected" + numMessage + "messages published in queue", numMessage,
                msgClient.queryMessageCount(queueName));

    }
}

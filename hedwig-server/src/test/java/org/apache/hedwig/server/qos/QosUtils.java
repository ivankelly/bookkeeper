package org.apache.hedwig.server.qos;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class QosUtils {

    /**
     * @param args
     */
    protected static final Logger logger = LoggerFactory.getLogger(QosUtils.class);
    static AtomicInteger numReceived = new AtomicInteger(0);
    static CountDownLatch receiveLatch = new CountDownLatch(1);
    static Message message = null;
    static int count = 0;

    /**
     * This api is for test publishing messages
     * 
     * @param args
     * @throws Exception
     */
    public void testMessageQueueClient_pub(final String queueName, final int numMessages) throws Exception {
        final MsgBusClient msgBusClient = new MsgBusClient(new TestHubUnavailable(true).new TestClientConfiguration());
        final MessageQueueClient client = msgBusClient.getMessageQueueClient();

        int length = 1;
        StringBuffer sb = new StringBuffer("");
        for (int i = 0; i < length; i++) {
            sb.append("a");
        }
        final String prefix = sb.toString();

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);

        long start = System.currentTimeMillis();

        logger.info("Start to asynsPublish!");
        client.createQueue(queueName);
        client.publish(queueName, prefix + 1);
        if (numMessages == numPublished.incrementAndGet()) {
            publishLatch.countDown();
        }

        for (int i = 2; i <= numMessages; i++) {

            final String str = prefix + i;

            client.asyncPublishWithResponse(queueName, str, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    // map, same message content results wrong
                    // publishedMsgs.put(str,
                    // response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                }
            }, null);
        }
        long end = System.currentTimeMillis();

        // wait the work to finish
        assertTrue("Timed out waiting on callback for publish requests.", publishLatch.await(3, TimeUnit.SECONDS));

        logger.info("AsyncPublished " + numMessages + " messages in " + (end - start) + " ms.");
    }

    public void recv_forConsumerCluster(String[] args) throws Exception {
        logger.info("enter.........................rec");
        // java.security.Security.setProperty("networkaddress.cache.ttl", "0");
        final String queueName = args[0];
        final int numMessages = Integer.parseInt(args[1]);

        if ("0" == args[2])
            logger.info(args[2] + "........................");
        final MsgBusClient client = new MsgBusClient(this.getClass().getResource("/hw_client.conf"));
        final MessageQueueClient mqClient = client.getMessageQueueClient();
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(50).build();
        logger.info("numMessages:::::" + numMessages);
        mqClient.createQueue(queueName);
        long start = System.currentTimeMillis();
        mqClient.startDelivery(queueName, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                logger.info(Thread.currentThread().getName() + "::deliver.." + msg.getMsgId().getLocalComponent()
                        + "::" + msg.getBody().toStringUtf8());

                if (numMessages == numReceived.incrementAndGet()) {
                    try {
                        mqClient.stopDelivery(queueName);
                    } catch (ClientNotSubscribedException e) {
                    }
                    receiveLatch.countDown();
                }

                try {
                    mqClient.consumeMessage(queueName, msg.getMsgId());
                    logger.info(Thread.currentThread().getName() + ":: consume.." + msg.getMsgId().getLocalComponent()
                            + "::" + msg.getBody().toStringUtf8());
                } catch (ClientNotSubscribedException e) {
                    e.printStackTrace();
                }

            }
        }, options);
        if (args[2] == "0") {
            assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(10, TimeUnit.SECONDS));
            mqClient.stopDelivery(queueName);
            mqClient.closeSubscription(queueName);
        }

        else {
            TimeUnit.MILLISECONDS.sleep(50);
            client.close();
            // mqClient.closeSubscription(queueName);
            logger.info(".........one client quit..............." + Thread.currentThread().getName());
        }

        long end = System.currentTimeMillis();
        logger.info(Thread.currentThread().getName() + "..........receiving finished.");
        logger.info("numReceived:" + numReceived.get());
        logger.info(Thread.currentThread().getName() + "::Time cost for receiving is " + (end - start) + " ms.");
    }

    public void recv_forResendOverTimeout(String[] args) throws Exception {
        logger.info("enter.........................rec");
        // java.security.Security.setProperty("networkaddress.cache.ttl", "0");
        final String queueName = args[0];
        final int numMessages = Integer.parseInt(args[1]);

        final MsgBusClient client = new MsgBusClient(this.getClass().getResource("/hw_client.conf"));
        final MessageQueueClient mqClient = client.getMessageQueueClient();
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(50).build();
        logger.info("numMessages:::::" + numMessages);
        mqClient.createQueue(queueName);
        long start = System.currentTimeMillis();

        mqClient.startDelivery(queueName, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {
                count++;
                logger.info("........................." + count);
                logger.info("client fetch local.." + msg.getMsgId().getLocalComponent() + "::"
                        + msg.getBody().toStringUtf8());

                if (numMessages == numReceived.incrementAndGet()) {
                    try {
                        mqClient.stopDelivery(queueName);
                    } catch (ClientNotSubscribedException e) {
                    }
                    receiveLatch.countDown();
                }

                if (msg.getMsgId().getLocalComponent() != 10) {
                    try {
                        mqClient.consumeMessage(queueName, msg.getMsgId());
                        logger.info("::client has call consume.." + msg.getMsgId().getLocalComponent() + "::"
                                + msg.getBody().toStringUtf8());
                    } catch (ClientNotSubscribedException e) {
                        e.printStackTrace();
                    }
                } else {
                    logger.info(msg.getBody().toStringUtf8() + ".........message not consumed");
                    message = msg;

                }
                if (msg.getMsgId().getLocalComponent() == 50) {

                    try {
                        mqClient.consumeMessage(queueName, message.getMsgId());
                        logger.info("::client has call consume message10.." + message.getMsgId().getLocalComponent()
                                + "::" + message.getBody().toStringUtf8());
                    } catch (ClientNotSubscribedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, options);

        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(20, TimeUnit.SECONDS));
        mqClient.stopDelivery(queueName);
        mqClient.closeSubscription(queueName);

        long end = System.currentTimeMillis();
        logger.info(Thread.currentThread().getName() + "..........receiving finished.");
        logger.info("numReceived:" + numReceived.get());
        logger.info(Thread.currentThread().getName() + "::Time cost for receiving is " + (end - start) + " ms.");
    }

    public void recv_forResendOverDisconnect(String[] args) throws Exception {
        logger.info("enter.........................rec");
        // java.security.Security.setProperty("networkaddress.cache.ttl", "0");
        final String queueName = args[0];
        final int numMessages = Integer.parseInt(args[1]);

        final MsgBusClient client = new MsgBusClient(this.getClass().getResource("/hw_client.conf"));
        final MessageQueueClient mqClient = client.getMessageQueueClient();
        final SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(50).build();
        logger.info("numMessages:::::" + numMessages);
        mqClient.createQueue(queueName);
        long start = System.currentTimeMillis();

        mqClient.startDelivery(queueName, new MessageHandler() {

            @Override
            synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                    Callback<Void> callback, Object context) {

                logger.info("client fetch local.." + msg.getMsgId().getLocalComponent() + "::"
                        + msg.getBody().toStringUtf8());

                if (numMessages == numReceived.incrementAndGet()) {
                    try {
                        mqClient.stopDelivery(queueName);
                    } catch (ClientNotSubscribedException e) {
                    }
                    receiveLatch.countDown();
                }

                try {
                    mqClient.consumeMessage(queueName, msg.getMsgId());
                    logger.info("::client has call consume.." + msg.getMsgId().getLocalComponent() + "::"
                            + msg.getBody().toStringUtf8());
                } catch (ClientNotSubscribedException e) {
                    e.printStackTrace();
                }

            }
        }, options);
        // ///////////////////////////////////////////////////////////////

        TimeUnit.MILLISECONDS.sleep(10);
        logger.info("simulate client disconnect................");
        client.close();
        // mqClient.closeSubscription(queueName);
        TimeUnit.MILLISECONDS.sleep(10);
        logger.info("simulate client try to reconnect................");
        final MsgBusClient client1 = new MsgBusClient(this.getClass().getResource("/hw_client.conf"));
        final MessageQueueClient mqClient1 = client1.getMessageQueueClient();
        Thread t1 = new Thread() {

            @Override
            public void run() {
                try {
                    // TODO Auto-generated method stub

                    mqClient1.createQueue(queueName);
                    mqClient1.startDelivery(queueName, new MessageHandler() {

                        @Override
                        synchronized public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                                Callback<Void> callback, Object context) {

                            logger.info("client fetch local.." + msg.getMsgId().getLocalComponent() + "::"
                                    + msg.getBody().toStringUtf8());

                            if (numMessages == numReceived.incrementAndGet()) {
                                try {
                                    mqClient1.stopDelivery(queueName);
                                } catch (ClientNotSubscribedException e) {
                                }
                                receiveLatch.countDown();
                            }

                            try {
                                mqClient1.consumeMessage(queueName, msg.getMsgId());
                                logger.info("::client has call consume.." + msg.getMsgId().getLocalComponent() + "::"
                                        + msg.getBody().toStringUtf8());
                            } catch (ClientNotSubscribedException e) {
                                e.printStackTrace();
                            }

                        }
                    }, options);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        };
        t1.start();
        // /////////////////////////////////////////////////////////////////
        assertTrue("Timed out waiting on callback for messages.", receiveLatch.await(13, TimeUnit.SECONDS));
        // mqClient.stopDelivery(queueName);
        mqClient1.closeSubscription(queueName);

        long end = System.currentTimeMillis();
        logger.info(Thread.currentThread().getName() + "..........receiving finished.");
        logger.info("numReceived:" + numReceived.get());
        logger.info(Thread.currentThread().getName() + "::Time cost for receiving is " + (end - start) + " ms.");
    }

}

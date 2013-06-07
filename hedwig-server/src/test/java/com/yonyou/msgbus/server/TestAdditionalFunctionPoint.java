package com.yonyou.msgbus.server;

import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
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
import org.apache.hedwig.server.HedwigHubTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.junit.Ignore;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

public class TestAdditionalFunctionPoint extends HedwigHubTestBase {

    boolean isSubscriptionChannelSharingEnabled = false;

    protected class TestServerConfiguration extends HubServerConfiguration {

        TestServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }
    }

    protected class TestClientConfiguration extends HubClientConfiguration {

        int messageWindowSize;

        TestClientConfiguration() {
            this.messageWindowSize = 100;
        }

        TestClientConfiguration(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public int getMaximumOutstandingMessages() {
            return messageWindowSize;
        }

        void setMessageWindowSize(int messageWindowSize) {
            this.messageWindowSize = messageWindowSize;
        }

        @Override
        public boolean isAutoSendConsumeMessageEnabled() {
            return false;
        }

        @Override
        public boolean isSubscriptionChannelSharingEnabled() {
            return isSubscriptionChannelSharingEnabled;
        }
    }

    @Override
    protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
        return new TestServerConfiguration(port, sslPort);
    }

    public static String createString(int size) {
        char[] chars = new char[size];
        // Optional step - unnecessary if you're happy with the array being full
        // of \0
        Arrays.fill(chars, 'a');

        return new String(chars);
    }

    @Ignore
    public void messageCountWithoutConsume() {
        // Message msg =
        // Message.newBuilder().setBody(ByteString.copyFromUtf8(createString(256))).build();
        MsgBusClient client = new MsgBusClient(new TestClientConfiguration());
        MessageQueueClient queue = client.getMessageQueueClient();

        final long numMessages = 100;
        final AtomicInteger numPublished = new AtomicInteger(0);
        String prefix = "test-", queueName = "testQueue-1";
        final CountDownLatch publishLatch = new CountDownLatch(1);
        try {
            queue.createQueue("testQueue-1");
        } catch (InterruptedException e) {
            return;
        }
        String str;
        for (int i = 0; i < numMessages; i++) {

            str = prefix + i;

            queue.asyncPublishWithResponse(queueName, str, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    // map, same message content results wrong
                    // publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                }
            }, null);
        }
        long count = 0;
        try {
            count = queue.queryMessageCount(queueName);
        } catch (InterruptedException e) {
            fail("Interrupted accidentally");
        }
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
        }
        client.close();
        org.junit.Assert.assertThat(count, equalTo(numMessages));
    }

    public void testMessageCountWithConsume() {
        // Message msg =
        // Message.newBuilder().setBody(ByteString.copyFromUtf8(createString(256))).build();
        MsgBusClient client = new MsgBusClient(new TestClientConfiguration());
        final MessageQueueClient queue = client.getMessageQueueClient();

        final long numMessages = 100, numToReceive = 50;
        final AtomicInteger numPublished = new AtomicInteger(0);
        final String prefix = "test-", queueName = "testQueue-1";
        final CountDownLatch publishLatch = new CountDownLatch(1);
        try {
            queue.createQueue("testQueue-1");
        } catch (InterruptedException e) {
            return;
        }
        String str;
        for (int i = 0; i < numMessages; i++) {

            str = prefix + i;

            queue.asyncPublishWithResponse(queueName, str, new Callback<PublishResponse>() {
                @Override
                public void operationFinished(Object ctx, PublishResponse response) {
                    // map, same message content results wrong
                    // publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
                public void operationFailed(Object ctx, final PubSubException exception) {
                }
            }, null);
        }
        SubscriptionOptions options = SubscriptionOptions.newBuilder()
                .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH).setEnableResubscribe(false)
                .setMessageWindowSize(1000).build();
        final CountDownLatch receiveLatch = new CountDownLatch(1);
        final AtomicInteger numReceived = new AtomicInteger(0);
        try {
            queue.startDelivery(queueName, new MessageHandler() {

                @Override
                public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                        Object context) {

                    System.out.println(msg.getBody().toStringUtf8() + " " + msg.getMsgId().getLocalComponent());
                    if (numToReceive == numReceived.incrementAndGet()) {
                        receiveLatch.countDown();
                        try {
                            queue.stopDelivery(queueName);
                        } catch (ClientNotSubscribedException e) {
                        }
                    }

                    try {
                        queue.consumeMessage(queueName, msg.getMsgId());
                    } catch (ClientNotSubscribedException e) {
                        e.printStackTrace();
                    }

                }
            }, options);
        } catch (Exception e) {

        }
        long count = 0;
        try {
            receiveLatch.await();
            count = queue.queryMessageCount(queueName);
        } catch (InterruptedException e) {
            fail("Interrupted accidentally");
        }
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
        }
        client.close();
        org.junit.Assert.assertThat(count, equalTo(numMessages - numToReceive));
    }

}

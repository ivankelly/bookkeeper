/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.filter.MessageFilterBase;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.server.PubSubServerStandAloneTestBase;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MsgBusClient;
import com.yonyou.msgbus.client.PubSubClient;

@RunWith(Parameterized.class)
public class TestPubSubClient extends PubSubServerStandAloneTestBase {
	protected static Logger logger = LoggerFactory
			.getLogger(TestPubSubClient.class);
    private static final int RETENTION_SECS_VALUE = 10;

    // Client side variables
	// protected HedwigClient client;
	// protected Publisher publisher;
	// protected Subscriber subscriber;
	protected MsgBusClient mbclient;
	protected PubSubClient client;

	protected class RetentionServerConfiguration extends
			StandAloneServerConfiguration {
        @Override
        public boolean isStandalone() {
            return true;
        }

        @Override
        public int getRetentionSecs() {
            return RETENTION_SECS_VALUE;
        }
    }

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();
	private final SynchronousQueue<SubscriptionEvent> eventQueue = new SynchronousQueue<SubscriptionEvent>();

    // Test implementation of Callback for async client actions.
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
	class TestMessageHandlerWithConsume implements MessageHandler {

		private final SynchronousQueue<Boolean> consumeQueue;

		public TestMessageHandlerWithConsume() {
			this.consumeQueue = TestPubSubClient.this.consumeQueue;
		}

		public TestMessageHandlerWithConsume(
				SynchronousQueue<Boolean> consumeQueue) {
			this.consumeQueue = consumeQueue;
		}

		public void deliver(final ByteString topic,
				final ByteString subscriberId, final Message msg,
				Callback<Void> callback, Object context) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					logger.info("enter....................deliver");
					logger.info(msg.getBody().toStringUtf8());
					try {
						client.consume(topic.toStringUtf8(),
								subscriberId.toStringUtf8(), msg.getMsgId());
						if (logger.isDebugEnabled())
							logger.debug("Consume operation finished successfully!");
						ConcurrencyUtils
								.put(TestMessageHandlerWithConsume.this.consumeQueue,
										true);
					} catch (ClientNotSubscribedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}).start();
			callback.operationFinished(context, null);
		}
	}

    class TestMessageHandler implements MessageHandler {

        private final SynchronousQueue<Boolean> consumeQueue;

        public TestMessageHandler() {
            this.consumeQueue = TestPubSubClient.this.consumeQueue;
        }

        public TestMessageHandler(SynchronousQueue<Boolean> consumeQueue) {
            this.consumeQueue = consumeQueue;
        }

		public void deliver(final ByteString topic,
				final ByteString subscriberId, final Message msg,
				Callback<Void> callback, Object context) {
            new Thread(new Runnable() {
                @Override
                public void run() {
					logger.info("enter....................deliver");
					logger.info(msg.getBody().toStringUtf8());
                    if (logger.isDebugEnabled())
                        logger.debug("Consume operation finished successfully!");
					ConcurrencyUtils.put(TestMessageHandler.this.consumeQueue,
							true);
                }
            }).start();
            callback.operationFinished(context, null);
        }
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true }, { false } });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestPubSubClient(boolean isSubscriptionChannelSharingEnabled) {
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
		mbclient = new MsgBusClient(this.getClass().getResource(
				"/hw_client.conf"));
		client = mbclient.getPubSubClient();
    }

    @Override
    @After
    public void tearDown() throws Exception {
		mbclient.close();
        super.tearDown();
    }

	@Test(timeout = 60000)
    public void testSyncPublish() throws Exception {
		logger.info("testSyncPublish...................");
        boolean publishSuccess = true;
        try {
			client.publish("mySyncTopic", "Hello Sync World!");
        } catch (Exception e) {
            publishSuccess = false;
        }
        assertTrue(publishSuccess);
    }

	@Test(timeout = 60000)
    public void testAsyncPublish() throws Exception {
		logger.info("testAsyncPublish...................");
		client.asyncPublish("myAsyncTopic", "Hello Async World!",
				new TestCallback(), null);
        assertTrue(queue.take());
    }

	@Test(timeout = 60000)
    public void testAsyncPublishWithResponse() throws Exception {
		logger.info("testAsyncPublishWithResponse...................");
		String topic = "testAsyncPublishWithResponse";
		String subid = "mysubid";

        final String prefix = "AsyncMessage-";
        final int numMessages = 30;

        final AtomicInteger numPublished = new AtomicInteger(0);
        final CountDownLatch publishLatch = new CountDownLatch(1);
		final Map<String, MessageSeqId> publishedMsgs = new HashMap<String, MessageSeqId>();

        final AtomicInteger numReceived = new AtomicInteger(0);
        final CountDownLatch receiveLatch = new CountDownLatch(1);
		final Map<String, MessageSeqId> receivedMsgs = new HashMap<String, MessageSeqId>();

		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();

		client.subscribe(topic, subid, options);
		client.startDelivery(topic, subid, new MessageHandler() {
			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
                String str = msg.getBody().toStringUtf8();
                receivedMsgs.put(str, msg.getMsgId()); 
                if (numMessages == numReceived.incrementAndGet()) {
                    receiveLatch.countDown();
                }
                callback.operationFinished(context, null);
            }
        });

		for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
			// ByteString data = ByteString.copyFromUtf8(str);
			// Message msg = Message.newBuilder().setBody(data).build();
			client.asyncPublishWithResponse(topic, str,
					new Callback<PublishResponse>() {
                @Override
						public void operationFinished(Object ctx,
								PublishResponse response) {
                    publishedMsgs.put(str, response.getPublishedMsgId());
                    if (numMessages == numPublished.incrementAndGet()) {
                        publishLatch.countDown();
                    }
                }

                @Override
						public void operationFailed(Object ctx,
								final PubSubException exception) {
                    publishLatch.countDown();
                }
            }, null);
        }
        assertTrue("Timed out waiting on callback for publish requests.",
                   publishLatch.await(10, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " publishes.",
                     numMessages, numPublished.get());
		assertEquals("Should be expected " + numMessages
				+ " publishe responses.", numMessages, publishedMsgs.size());

        assertTrue("Timed out waiting on callback for messages.",
                   receiveLatch.await(30, TimeUnit.SECONDS));
        assertEquals("Should be expected " + numMessages + " messages.",
                     numMessages, numReceived.get());
        assertEquals("Should be expected " + numMessages + " messages in map.",
                     numMessages, receivedMsgs.size());

		for (int i = 0; i < numMessages; i++) {
            final String str = prefix + i;
            MessageSeqId pubId = publishedMsgs.get(str);
            MessageSeqId revId = receivedMsgs.get(str);
            assertTrue("Doesn't receive same message seq id for " + str,
                       pubId.equals(revId));
        }
    }

	@Test(timeout = 60000)
    public void testSyncSubscribe() throws Exception {
		logger.info("testSyncSubscribe...................");
        boolean subscribeSuccess = true;
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
        try {
			client.subscribe("mySyncSubscribeTopic", "1", options);
        } catch (Exception e) {
            subscribeSuccess = false;
        }
        assertTrue(subscribeSuccess);
    }

	@Test(timeout = 60000)
    public void testAsyncSubscribe() throws Exception {
		logger.info("testAsyncSubscribe...................");
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		client.asyncSubscribe("myAsyncSubscribeTopic", "1", options,
				new TestCallback(), null);
        assertTrue(queue.take());
    }

	@Test(timeout = 60000)
	public void testAsyncUnsubscribe() throws Exception {
		logger.info("testAsyncUnsubscribe...................");
		String topic = "myAsyncUnsubTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();

		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(),
				null);
        assertTrue(queue.take());
		client.asyncUnsubscribe(topic, subscriberId, new TestCallback(), null);
        assertTrue(queue.take());
    }

	@Test(timeout = 60000)
	public void testSyncUnsubscribe() throws Exception {
		logger.info("testSyncUnsubscribe...................");
		boolean unSubscribeSuccess = true;
		String topic = "mySyncUnsubTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();

		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(),
				null);
		assertTrue(queue.take());
        try {
			client.unsubscribe(topic, subscriberId);
		} catch (Exception e) {
			unSubscribeSuccess = false;
        }
		assertTrue(unSubscribeSuccess);
    }

	@Test(timeout = 60000)
	public void testCloseSubscription() throws Exception {
		logger.info("testCloseSubscription...................");
		boolean closeSubscriptionSuccess = true;
		String topic = "myCloseSubscriptionTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(),
				null);
            assertTrue(queue.take());
		try {
			client.closeSubscription(topic, subscriberId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			closeSubscriptionSuccess = false;
        }
		assertTrue(closeSubscriptionSuccess);
    }

	@Test(timeout = 60000)
	public void testAsyncCloseSubscription() throws Exception {
		logger.info("testAsyncCloseSubscription...................");

		String topic = "myAsyncCloseSubscriptionTopic";
		String subscriberId = "1";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(),
				null);
		assertTrue(queue.take());
		client.asyncCloseSubscription(topic, subscriberId, new TestCallback(),
				null);
        assertTrue(queue.take());

        }

	@Test(timeout = 60000)
	public void testStartDelivery() throws Exception {
		logger.info("testStartDelivery...................");
		String topic = "testStartDelivery";
		String subid = "mysubid";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();

		client.subscribe(topic, subid, options);

		// Start delivery for the subscriber
		client.startDelivery(topic, subid, new TestMessageHandler());
        // Now publish some messages for the topic to be consumed by the
        // subscriber.
		client.publish(topic, "Message #1");
		assertTrue(consumeQueue.take());
	}

	@Test(timeout = 60000)
	public void testStartDeliveryWithFilter() throws Exception {
		logger.info("testStartDeliveryWithFilter...................");
		String topic = "testStartDeliveryWithFilter";
		String subid = "mysubid";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();

		client.subscribe(topic, subid, options);

		// Start delivery for the subscriber
		client.startDeliveryWithFilter(topic, subid, new TestMessageHandler(),
				new ClientMessageFilter() {

            @Override
					public MessageFilterBase setSubscriptionPreferences(
							ByteString topic, ByteString subscriberId,
							SubscriptionPreferences preferences) {
						// TODO Auto-generated method stub
						return null;
            }

            @Override
					public boolean testMessage(Message message) {
						// TODO Auto-generated method stub
						if (!message.getBody().toStringUtf8().endsWith("1")) {
							logger.info(message.getBody().toStringUtf8()
									+ "...................");
							return false;
						} else {
							return true;
            }
        }

				});
		// Now publish some messages for the topic to be consumed by the
		// subscriber.
		client.publish(topic, "Message #1");
        assertTrue(consumeQueue.take());
    }

	@Test(timeout = 60000)
	public void testStopDelivery() throws Exception {
		logger.info("testStopDelivery...................");
		String topic = "testStopDelivery";
		String subid = "mysubid";
		boolean StopDeliverySuccess = true;
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();

		client.subscribe(topic, subid, options);

		// Start delivery for the subscriber
		client.startDelivery(topic, subid, new TestMessageHandler());
		// Now publish some messages for the topic to be consumed by the
		// subscriber.
		client.publish(topic, "Message #1");

		try {
			client.stopDelivery(topic, subid);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			StopDeliverySuccess = false;
		}
		assertTrue(StopDeliverySuccess);
            }

	@Test(timeout = 60000)
	public void testConsume() throws Exception {
		logger.info("testConsume...................");
		String topic = "myConsumeTopic";
		String subscriberId = "1";
		String prefix = "Message-";
		SubscriptionOptions options = SubscriptionOptions.newBuilder()
            .setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		client.asyncSubscribe(topic, subscriberId, options, new TestCallback(),
				null);
		assertTrue(queue.take());

		// Start delivery for the subscriber
		client.startDelivery(topic, subscriberId,
				new TestMessageHandlerWithConsume());

		// Now publish some messages for the topic to be consumed by the
		// subscriber.
		for (int i = 0; i < 5; i++) {
			String str = prefix + i;
			client.asyncPublish(topic, str, new TestCallback(), null);
			assertTrue(queue.take());
			assertTrue(consumeQueue.take());
		}

	}

	@Test(timeout = 60000)
	public void testCloseClient() throws Exception {
		logger.info("testCloseClient...................");
		boolean CloseClientSuccess = true;
		String topic = "myCloseClientTopic";
		String subscriberId = "1";

		SubscriptionOptions options = SubscriptionOptions.newBuilder()
				.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
				.setEnableResubscribe(false).setMessageWindowSize(50).build();
		client.subscribe(topic, subscriberId, options);

		try {
			mbclient.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			CloseClientSuccess = false;
    }
		assertTrue(CloseClientSuccess);

	}
}

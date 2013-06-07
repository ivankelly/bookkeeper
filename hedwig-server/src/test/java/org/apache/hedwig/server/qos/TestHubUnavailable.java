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
package org.apache.hedwig.server.qos;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.server.HedwigHubTestBase2;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.util.Callback;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;
import com.yonyou.msgbus.client.MessageQueueClient;
import com.yonyou.msgbus.client.MsgBusClient;

@RunWith(Parameterized.class)
public class TestHubUnavailable extends HedwigHubTestBase2 {

	private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 1000;

	protected class TestServerConfiguration extends HubServerConfiguration {

		TestServerConfiguration(int serverPort, int sslServerPort) {
			super(serverPort, sslServerPort);
		}

		@Override
		public int getDefaultMessageWindowSize() {
			return TestHubUnavailable.DEFAULT_MESSAGE_WINDOW_SIZE;
		}
	}

	protected class TestClientConfiguration extends HubClientConfiguration {

		int messageWindowSize;

		TestClientConfiguration() {
			this.messageWindowSize = 50;
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

	@Parameters
	public static Collection<Object[]> configs() {
		return Arrays.asList(new Object[][] { { true } /* , { true } */});
	}

	protected boolean isSubscriptionChannelSharingEnabled;

	public TestHubUnavailable(boolean isSubscriptionChannelSharingEnabled) {

		// super(2); //default numServers is 2 :set in super class
		this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
		System.out.println("enter ..........................constructor");
	}

	@BeforeClass
	public static void oneTimeSetUp() {
		// one-time initialization code
		System.setProperty("build.test.dir", "F:\\logDir");
	}

	@Override
	@Before
	public void setUp() throws Exception {
		super.setUp();
	}

	@Override
	protected ServerConfiguration getServerConfiguration(int port, int sslPort) {
		return new TestServerConfiguration(port, sslPort);
	}

	protected class RecvConfiguration {

		AtomicInteger numReceived = new AtomicInteger(0);
		CountDownLatch receiveLatch = new CountDownLatch(1);
		String queueName = "messageQueue-test";
		int numMessages = 500;
		SubscriptionOptions options;

		RecvConfiguration(String queueName, int numMessages,
				int messageWindowSize) {
			this.queueName = queueName;
			this.numMessages = numMessages;
			this.options = SubscriptionOptions.newBuilder()
					.setCreateOrAttach(CreateOrAttach.CREATE_OR_ATTACH)
					.setEnableResubscribe(false)
					.setMessageWindowSize(messageWindowSize).build();
		}

		public AtomicInteger getNumReceived() {
			return numReceived;
		}

		public void setNumReceived(AtomicInteger numReceived) {
			this.numReceived = numReceived;
		}

		public CountDownLatch getReceiveLatch() {
			return receiveLatch;
		}

		public void setReceiveLatch(CountDownLatch receiveLatch) {
			this.receiveLatch = receiveLatch;
		}

		public String getQueueName() {
			return queueName;
		}

		public void setQueueName(String queueName) {
			this.queueName = queueName;
		}

		public int getNumMessages() {
			return numMessages;
		}

		public void setNumMessages(int numMessages) {
			this.numMessages = numMessages;
		}

		public SubscriptionOptions getOptions() {
			return options;
		}

		public void setOptions(SubscriptionOptions options) {
			this.options = options;
		}

	}

	/*
	 * A hub is shutdown while receiving messages,then we create another client
	 * and register it to another available hub.We will test whether the second
	 * hub can take the topic on the unavailable hub over.
	 */
	public void topicOwnershipChangeOverHubUnavailable() throws Exception {
		final PubSubServer defaultServer = super.getServer().get(0);
		final RecvConfiguration rcConfig = new RecvConfiguration(
				"messageQueue-test", 500, 500);

		final MsgBusClient client = new MsgBusClient(this.getClass()
				.getResource("/hw_client.conf"));
		final MessageQueueClient mqClient = client.getMessageQueueClient();
		mqClient.createQueue(rcConfig.queueName);
		long start = System.currentTimeMillis();

		mqClient.startDelivery(rcConfig.queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				logger.info("client fetch local.."
						+ msg.getMsgId().getLocalComponent() + "::"
						+ msg.getBody().toStringUtf8());

				if (rcConfig.numMessages == rcConfig.numReceived
						.incrementAndGet()) {
					try {
						mqClient.stopDelivery(rcConfig.queueName);

					} catch (ClientNotSubscribedException e) {
					}
					rcConfig.receiveLatch.countDown();
				}
				try {
					System.out.println(Thread.currentThread().getName()
							+ ":: consume.."
							+ msg.getMsgId().getLocalComponent() + "::"
							+ msg.getBody().toStringUtf8());
					mqClient.consumeMessage(rcConfig.queueName, msg.getMsgId());

				} catch (ClientNotSubscribedException e) {
					e.printStackTrace();
				}

			}
		}, rcConfig.options);

		TimeUnit.MILLISECONDS.sleep(10);
		logger.info("will shut down default server.........");
		defaultServer.shutdown();
		mqClient.closeSubscription(rcConfig.queueName);
		logger.info("default server is shut down.............");
		TimeUnit.MILLISECONDS.sleep(10);

		final MsgBusClient client1 = new MsgBusClient(this.getClass()
				.getResource("/hw_client1.conf"));
		final MessageQueueClient mqClient1 = client1.getMessageQueueClient();
		mqClient1.createQueue(rcConfig.queueName);
		logger.info("mq1 finish create.................");
		mqClient1.startDelivery(rcConfig.queueName, new MessageHandler() {

			@Override
			synchronized public void deliver(ByteString topic,
					ByteString subscriberId, Message msg,
					Callback<Void> callback, Object context) {
				logger.info("client fetch local.."
						+ msg.getMsgId().getLocalComponent() + "::"
						+ msg.getBody().toStringUtf8());

				if (rcConfig.numMessages == rcConfig.numReceived
						.incrementAndGet()) {
					try {
						mqClient1.stopDelivery(rcConfig.queueName);

					} catch (ClientNotSubscribedException e) {
					}
					rcConfig.receiveLatch.countDown();
				}
				try {
					System.out.println(Thread.currentThread().getName()
							+ ":: consume.."
							+ msg.getMsgId().getLocalComponent() + "::"
							+ msg.getBody().toStringUtf8());
					mqClient1.consumeMessage(rcConfig.queueName, msg.getMsgId());

				} catch (ClientNotSubscribedException e) {
					e.printStackTrace();
				}

			}
		}, rcConfig.options);
		System.out.println("mq1 start deliver.................");
		assertTrue("Timed out waiting on callback for messages.",
				rcConfig.receiveLatch.await(15, TimeUnit.SECONDS));
		mqClient1.closeSubscription(rcConfig.queueName);
		long end = System.currentTimeMillis();
		logger.info(Thread.currentThread().getName()
				+ "..........receiving finished.");
		logger.info("numReceived:" + rcConfig.numReceived.get());
		logger.info(Thread.currentThread().getName()
				+ "::Time cost for receiving is " + (end - start) + " ms.");
	}

	@Test
	public void testHubUnavailable() throws Exception {
		new QosUtils().testMessageQueueClient_pub("messageQueue-test", 500);
		topicOwnershipChangeOverHubUnavailable();
		Thread.sleep(20000);
	}
}

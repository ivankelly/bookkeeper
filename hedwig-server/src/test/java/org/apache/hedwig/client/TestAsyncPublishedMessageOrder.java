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


import org.apache.hedwig.server.PubSubServerStandAloneTestBase;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.PubSubServer;
import org.apache.hedwig.util.Callback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class TestAsyncPublishedMessageOrder extends PubSubServerStandAloneTestBase {

    private static CountDownLatch signalAllReceived;
    static final int NB_MESSAGES = 100;
    List<String> invalidOrderings = new ArrayList<String>();

    @Test
    public void testAsyncPublish() throws Exception, ConfigurationException {
        doTestPublishing(true);
    }

    @Test
    public void testSyncPublish() throws Exception {
        doTestPublishing(false);
    }

    private void doTestPublishing(boolean async) throws Exception {
        signalAllReceived = new CountDownLatch(NB_MESSAGES);

        ClientConfiguration clientConf = new ClientConfiguration();
        // start subscriber and publisher
        new Thread(new SubscriberRunnable(clientConf)).start();

        if (async) {
            new Thread(new AsyncPublisher(clientConf)).start();
        } else {
            new Thread(new SyncPublisher(clientConf)).start();
        }

        // check results
        signalAllReceived.await(10, TimeUnit.SECONDS);

        Assert.assertTrue("Some messages are out of order", invalidOrderings.isEmpty());
    }

    class AsyncPublisher implements Runnable {

        Publisher pub;

        public AsyncPublisher(ClientConfiguration configuration) {
            pub = new HedwigClient(configuration).getPublisher();
        }

        @Override
        public void run() {
            for (int i = 0; i < NB_MESSAGES; i++) {
                pub.asyncPublish(ByteString.copyFromUtf8("topic1"),
                                 PubSubProtocol.Message.newBuilder().setBody(ByteString.copyFromUtf8("message_" + i)).build(),
                                 new Callback<Void>() {
                                     @Override
                                     public void operationFailed(Object ctx, PubSubException exception) {
                                         exception.printStackTrace();
                                     }

                                     @Override
                                     public void operationFinished(Object ctx, Void resultOfOperation) {
                                         System.out.print(".");
                                     }
                                 }, null);
            }
        }
    }

    class SyncPublisher implements Runnable {
        Publisher pub;

        public SyncPublisher(ClientConfiguration configuration) {
            pub = new HedwigClient(configuration).getPublisher();
        }

        @Override
        public void run() {
            for (int i = 0; i < NB_MESSAGES; i++) {
                try {
                    pub.publish(ByteString.copyFromUtf8("topic1"),
                                PubSubProtocol.Message.newBuilder().setBody(ByteString.copyFromUtf8("message_" + i))
                                .build());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    class SubscriberRunnable implements Runnable {
        Subscriber sub;

        public SubscriberRunnable(ClientConfiguration configuration) {
            sub = new HedwigClient(configuration).getSubscriber();
        }

        @Override
        public void run() {
            try {
                sub.subscribe(ByteString.copyFromUtf8("topic1"), ByteString.copyFromUtf8("subscriber1"),
                              PubSubProtocol.SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH);
                MessageReceiver receiver = new MessageReceiver();
                sub.startDelivery(ByteString.copyFromUtf8("topic1"), ByteString.copyFromUtf8("subscriber1"), receiver);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public class MessageReceiver implements MessageHandler {

            AtomicInteger lastSeen = new AtomicInteger(0);

            public MessageReceiver() {
            }

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, PubSubProtocol.Message msg,
                                Callback<Void> callback, Object context) {
                // System.out.println("!" + counter + "/" +
                // msg.getBody().toStringUtf8());
                int count = Integer.valueOf(msg.getBody().toStringUtf8().split("_")[1]);
                if (lastSeen.get() > count) {
                    invalidOrderings.add(msg.getBody().toStringUtf8() + " < " + "message_" + lastSeen.get());
                    System.err.println("just received " + msg.getBody().toStringUtf8()
                                       + " <  already seen message_" + lastSeen.get());
                } else {
                    lastSeen.set(count);
                }
                signalAllReceived.countDown();
            }
        }
    }
}

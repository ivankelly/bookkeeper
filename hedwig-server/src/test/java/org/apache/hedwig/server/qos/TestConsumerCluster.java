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

import org.apache.hedwig.server.HedwigHubTestBase1;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestConsumerCluster extends HedwigHubTestBase1 {

    private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 10;

    protected class ConsumerClusterServerConfiguration extends HubServerConfiguration {

        ConsumerClusterServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        @Override
        public int getDefaultMessageWindowSize() {
            return TestConsumerCluster.DEFAULT_MESSAGE_WINDOW_SIZE;
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

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { true } /* , { true } */});
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestConsumerCluster(boolean isSubscriptionChannelSharingEnabled) {

        super(1);
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
        // System.out.println("enter ..........................constructor");
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
        return new ConsumerClusterServerConfiguration(port, sslPort);
    }

    @Test
    public void testConsumerCluster() throws Exception {

        class QueueConsumer1 implements Runnable {

            @Override
            public void run() {

                QosUtils t = new QosUtils();

                String[] myArgs = new String[3];
                myArgs[0] = "messageQueue-test";
                myArgs[1] = "500";
                myArgs[2] = "0";

                try {
                    t.recv_forConsumerCluster(myArgs);
                    logger.info(Thread.currentThread().getName() + ":quit.............recv");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
        class QueueConsumer2 implements Runnable {

            @Override
            public void run() {
                // TODO Auto-generated method stub
                QosUtils t = new QosUtils();

                String[] myArgs = new String[3];
                myArgs[0] = "messageQueue-test";
                myArgs[1] = "500";
                myArgs[2] = "1";

                try {
                    t.recv_forConsumerCluster(myArgs);
                    logger.info(Thread.currentThread().getName() + ":quit.............recv");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

        new QosUtils().testMessageQueueClient_pub("messageQueue-test", 1000);
        new Thread(new QueueConsumer1()).start();
        new Thread(new QueueConsumer2()).start();

        Thread.sleep(15000);
        logger.info("quit...........main");

    }
}

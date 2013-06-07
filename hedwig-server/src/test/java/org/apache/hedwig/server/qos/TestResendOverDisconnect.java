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
public class TestResendOverDisconnect extends HedwigHubTestBase1 {

    private static final int DEFAULT_MESSAGE_WINDOW_SIZE = 1000;

    protected class TestServerConfiguration extends HubServerConfiguration {

        TestServerConfiguration(int serverPort, int sslServerPort) {
            super(serverPort, sslServerPort);
        }

        @Override
        public int getDefaultMessageWindowSize() {
            return TestResendOverDisconnect.DEFAULT_MESSAGE_WINDOW_SIZE;
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

    public TestResendOverDisconnect(boolean isSubscriptionChannelSharingEnabled) {

        super(1);
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
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

    @Test
    public void testResendOverDisconnect() throws Exception {

        class QueueConsumer implements Runnable {

            @Override
            public void run() {
                QosUtils t = new QosUtils();

                String[] myArgs = new String[3];
                myArgs[0] = "messageQueue-test";
                myArgs[1] = "500";
                myArgs[2] = "0";

                try {
                    t.recv_forResendOverDisconnect(myArgs);
                    logger.info(Thread.currentThread().getName() + ":quit.............recv");
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }

        new QosUtils().testMessageQueueClient_pub("messageQueue-test", 500);
        new Thread(new QueueConsumer()).start();

        Thread.sleep(15000);
        logger.info("quit...........main");

    }
}

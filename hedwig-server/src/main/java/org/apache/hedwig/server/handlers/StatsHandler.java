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
/* lizhhb for test */
package org.apache.hedwig.server.handlers;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.netty.ServerStats.OpStats;
import org.apache.hedwig.server.netty.UmbrellaHandler;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.ZkTopicManager;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class StatsHandler extends BaseHandler {
	static Logger logger = LoggerFactory.getLogger(StatsHandler.class);

	private final PersistenceManager persistenceMgr;
	private final TopicManager topicMgr;

	public StatsHandler(ServerConfiguration cfg, TopicManager topicMgr, DeliveryManager deliveryManager,
			PersistenceManager persistenceMgr, SubscriptionManager subMgr,
			SubscriptionChannelManager subChannelMgr) {
		super(topicMgr, cfg);
		this.persistenceMgr = persistenceMgr;	
		this.topicMgr=topicMgr;
	}

	@Override
	public void handleRequestAtOwner(final PubSubRequest request, final Channel channel) {

		if(request.getType().equals(OperationType.QUEUE_TOPIC_OP)) {
			getAvailableHubs(new Callback<String>(){

				@Override
				public void operationFinished(Object ctx, String resultOfOperation) {
					Message message = Message.newBuilder().setBody(
							ByteString.copyFromUtf8(resultOfOperation)).build();
					PubSubResponse response = PubSubResponse.newBuilder()
							.setProtocolVersion(ProtocolVersion.VERSION_ONE)
							.setStatusCode(StatusCode.SUCCESS).setTxnId(request.getTxnId()).setMessage(message)
							.build();
					
					channel.write(response);
					
				}

				@Override
				public void operationFailed(Object ctx, PubSubException exception) {
					// What to do?			
				}
				
			},null);
		}			
	}

	/* msgbus  team */
	public void getAvailableHubs(Callback<String> cb, Object ctx) {		
		// Get the list of existing hosts		
		((ZkTopicManager)topicMgr).getAvailableHubs(cb, ctx);
	}
	/* msgbus  team */
}

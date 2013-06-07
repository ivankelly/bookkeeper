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
package org.apache.hedwig.server.handlers;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.QueueMgmtResponse;
import org.apache.hedwig.protocol.PubSubProtocol.QueueOperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protoextensions.PubSubResponseUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.netty.ServerStats;
import org.apache.hedwig.server.netty.UmbrellaHandler;
import org.apache.hedwig.server.netty.ServerStats.OpStats;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.AbstractSubscriptionManager;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.ZkTopicManager;
import org.apache.hedwig.util.Callback;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/* msgbus added this file */
public class QueueHandler extends BaseHandler {
    static Logger logger = LoggerFactory.getLogger(QueueHandler.class);

    private final PersistenceManager persistenceMgr;
    private final SubscriptionManager subMgr;
    private final DeliveryManager deliveryMgr;
    private final SubscriptionChannelManager subChannelMgr;

    // op stats
    private final OpStats subStats;

    public QueueHandler(ServerConfiguration cfg, TopicManager topicMgr, PersistenceManager persistenceMgr,
            DeliveryManager deliveryMgr, SubscriptionChannelManager subChannelMgr, SubscriptionManager subMgr) {
        super(topicMgr, cfg);
        this.persistenceMgr = persistenceMgr;
        this.subMgr = subMgr;
        this.deliveryMgr = deliveryMgr;
        this.subChannelMgr = subChannelMgr;
        subStats = ServerStats.getInstance().getOpStats(OperationType.SUBSCRIBE);
    }

    @Override
    public void handleRequestAtOwner(final PubSubRequest request, final Channel channel) {
        if (!request.hasQueueMgmtRequest()) {
            UmbrellaHandler.sendErrorResponseToMalformedRequest(channel, request.getTxnId(),
                    "Missing queueMgmt request data");            
            return;
        }
        
        QueueOperationType operationType = request.getQueueMgmtRequest().getType();

        switch (operationType) {
        case CREATE:
            createQueue(request, channel);
            break;
        case DELETE:
            deleteQueue(request, channel);
            break;
        case QUERY_MSG_CNT:
            logger.warn("Got a count query request!");
            queryMessageCount(request, channel);
            break;
        case QUERYHUBS:
            getAvailableHubs(request, channel);
        }
    }

    private void createQueue(final PubSubRequest request, final Channel channel) {
        final ByteString topic = request.getTopic();

        MessageSeqId seqId;
        try {
            seqId = persistenceMgr.getCurrentSeqIdForTopic(topic);
        } catch (ServerNotResponsibleForTopicException e) {
            channel.write(PubSubResponseUtils.getResponseForException(e, request.getTxnId())).addListener(
                    ChannelFutureListener.CLOSE);
            logger.error("Error getting current seq id for topic " + topic.toStringUtf8()
                    + " when processing subscribe request (txnid:" + request.getTxnId() + ") :", e);
            subStats.incrementFailedOps();
            ServerStats.getInstance().incrementRequestsRedirect();
            return;
        }

        final SubscribeRequest subRequest = request.getSubscribeRequest();
        final ByteString subscriberId = subRequest.getSubscriberId();

        MessageSeqId lastSeqIdPublished = MessageSeqId.newBuilder(seqId).setLocalComponent(seqId.getLocalComponent())
                .build();

        // just write subData.
        SubscribeRequest mySubRequest = SubscribeRequest.newBuilder(subRequest)
                .setSubscriberId(SubscriptionStateUtils.QUEUE_SUBID_BS).build();
        subMgr.serveSubscribeRequest(topic, mySubRequest, lastSeqIdPublished, new Callback<SubscriptionData>() {

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId())).addListener(
                        ChannelFutureListener.CLOSE);
                logger.error(
                        "Error serving subscribe request (" + request.getTxnId() + ") for (topic: "
                                + topic.toStringUtf8() + " , subscriber: " + subscriberId.toStringUtf8() + ")",
                                exception);
            }

            @Override
            public void operationFinished(Object ctx, SubscriptionData subData) {
                synchronized (channel) {
                    if (!channel.isConnected()) {
                        // channel got disconnected while we were processing the
                        // subscribe request,
                        // nothing much we can do in this case
                        subStats.incrementFailedOps();
                        return;
                    }
                }
                channel.write(PubSubResponseUtils.getSuccessResponse(request.getTxnId()));
                return;
            }
        }, null);
    }

    private void deleteQueue(final PubSubRequest request, final Channel channel) {
        final ByteString topic = request.getTopic();
        final ByteString subscriberId = SubscriptionStateUtils.QUEUE_SUBID_BS;

        subMgr.unsubscribe(topic, subscriberId, new Callback<Void>() {
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                if (StatusCode.CLIENT_NOT_SUBSCRIBED == exception.getCode()) {
                    // if ClientNotSubscribedException, still perform deleteTopicPersistenceInfo
                    if (true == deliveryMgr.deleteTopicPersistenceInfoRecursive(topic)) {
                        channel.write(PubSubResponseUtils.getSuccessResponse(request.getTxnId()));
                    } else {
                        PubSubException e = new PubSubException.ClientNotSubscribedException(
                                "Can't delete topic persistence info.");
                        channel.write(PubSubResponseUtils.getResponseForException(e, request.getTxnId()));
                    }
                } else {
                    channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId()));
                }
            }

            @Override
            public void operationFinished(Object ctx, Void resultOfOperation) {

                // we should not close the channel in delivery manager
                // since client waits the response for closeSubscription request
                // client side would close the channel
                deliveryMgr.stopServingSubscriber(topic, subscriberId, null, new Callback<Void>() {
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId()));
                    }

                    @Override
                    public void operationFinished(Object ctx, Void resultOfOperation) {
                        // remove the topic subscription from
                        // subscription channels
                        subChannelMgr.remove(new TopicSubscriber(topic, subscriberId), channel);

                        if (true == deliveryMgr.deleteTopicPersistenceInfoRecursive(topic)) {
                            channel.write(PubSubResponseUtils.getSuccessResponse(request.getTxnId()));
                        } else {
                            PubSubException e = new PubSubException.ClientNotSubscribedException(
                                    "Can't delete topic persistence info.");
                            channel.write(PubSubResponseUtils.getResponseForException(e, request.getTxnId()));
                        }

                    }
                }, ctx);
            }
        }, null);
    }

    // just a interface, not process in client, for efficiency consideration
    private void getAvailableHubs(final PubSubRequest request, final Channel channel) {        
        // Get the list of existing hosts
        ((ZkTopicManager)topicMgr).getAvailableHubs(new Callback<String>(){

            @Override
            public void operationFinished(Object ctx, String resultOfOperation) {
                QueueMgmtResponse queueResponse = QueueMgmtResponse.newBuilder().setHubList(resultOfOperation).build();
                ResponseBody body = ResponseBody.newBuilder().setQueueMgmtResponse(queueResponse).build();
                PubSubResponse response = PubSubResponse.newBuilder()
                        .setProtocolVersion(ProtocolVersion.VERSION_ONE)
                        .setStatusCode(StatusCode.SUCCESS).setTxnId(request.getTxnId()).setResponseBody(body).build();
                channel.write(response);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                // What to do?
            }

        }, null);
    }

    private void queryMessageCount(final PubSubRequest request, final Channel channel) {      
        
        final ByteString topic = request.getTopic();
        ((AbstractSubscriptionManager) subMgr).queryMessagesForTopic(topic, new Callback<Long>() {

            @Override
            public void operationFinished(Object ctx, Long resultOfOperation) {
                logger.info("count query request fulfilled!");
                QueueMgmtResponse queueResponse = QueueMgmtResponse.newBuilder().setMessageCount(resultOfOperation)
                        .build();
                ResponseBody body = ResponseBody.newBuilder().setQueueMgmtResponse(queueResponse).build();
                PubSubResponse response = PubSubResponse.newBuilder().setProtocolVersion(ProtocolVersion.VERSION_ONE)
                        .setStatusCode(StatusCode.SUCCESS).setTxnId(request.getTxnId()).setResponseBody(body).build();
                channel.write(response);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                channel.write(PubSubResponseUtils.getResponseForException(exception, request.getTxnId()));
            }
        }, null);
    }
 
}

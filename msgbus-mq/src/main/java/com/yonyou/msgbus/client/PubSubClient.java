package com.yonyou.msgbus.client;

import java.util.List;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;

import com.google.protobuf.ByteString;

/***
 * this class contains APIs for pub/sub client, which is fit for the pub/sub
 * model in original hedwig
 * 
 */
public class PubSubClient {

    public static final String TOPIC_PREFIX = "t_";

    private Publisher publisher;
    private Subscriber subscriber;

    // Invisible to users
    PubSubClient(Publisher publisher, Subscriber subscriber) {
        this.publisher = publisher;
        this.subscriber = subscriber;
    }

    /**
     * publish messages to certain topic in a synchronous way
     * 
     * @param topic
     * @param msg
     * @return PublishResponse
     * @throws CouldNotConnectException
     * @throws ServiceDownException
     */

    public PublishResponse publish(String topic, String msg) throws CouldNotConnectException, ServiceDownException {

        ByteString myTopic = ByteString.copyFromUtf8(topic);
        Message myMessage = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
        return publisher.publish(myTopic, myMessage);
    }

    /**
     * publish messages to certain topic in a asynchronous way
     * 
     * @param topic
     * @param msg
     * @param callback
     * @param context
     */
    public void asyncPublish(String topic, String msg, Callback<Void> callback, Object context) {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        Message myMsg = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
        publisher.asyncPublish(myTopic, myMsg, callback, context);
    }

    /**
     * publish messages to certain topic in a asynchronous way, a response will
     * be returned.
     * 
     * @param topic
     * @param msg
     * @param callback
     * @param context
     */

    public void asyncPublishWithResponse(String topic, String msg, Callback<PublishResponse> callback, Object context) {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        Message myMsg = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
        publisher.asyncPublishWithResponse(myTopic, myMsg, callback, context);
    }

    // Check the subscriberId in sub/pub operation
    protected void subscribe(String topic, String subscriberId, CreateOrAttach mode) throws CouldNotConnectException,
            ClientAlreadySubscribedException, ServiceDownException, InvalidSubscriberIdException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = checkAndConvertSubid(subscriberId);
        subscriber.subscribe(myTopic, mySubscriberId, mode);
    }

    // merge with above function
    public void subscribe(String topic, String subscriberId, SubscriptionOptions options)
            throws CouldNotConnectException, ClientAlreadySubscribedException, ServiceDownException,
            InvalidSubscriberIdException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = checkAndConvertSubid(subscriberId);
        subscriber.subscribe(myTopic, mySubscriberId, options);
    }

    protected void asyncSubscribe(String topic, String subscriberId, CreateOrAttach mode, Callback<Void> callback,
            Object context) {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = null;
        try {
            mySubscriberId = checkAndConvertSubid(subscriberId);
        } catch (InvalidSubscriberIdException e) {
            callback.operationFailed(context, new ServiceDownException(e));
        }
        SubscriptionOptions options = SubscriptionOptions.newBuilder().setCreateOrAttach(mode).build();
        subscriber.asyncSubscribe(myTopic, mySubscriberId, options, callback, context);
    }

    // merge with above function
    public void asyncSubscribe(String topic, String subscriberId, SubscriptionOptions options, Callback<Void> callback,
            Object context) {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = null;
        try {
            mySubscriberId = checkAndConvertSubid(subscriberId);
        } catch (InvalidSubscriberIdException e) {
            callback.operationFailed(context, new ServiceDownException(e));
        }
        subscriber.asyncSubscribe(myTopic, mySubscriberId, options, callback, context);
    }

    // this will call perform closeSubscription in advance
    public void unsubscribe(String topic, String subscriberId) throws CouldNotConnectException,
            ClientNotSubscribedException, ServiceDownException, InvalidSubscriberIdException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = checkAndConvertSubid(subscriberId);
        subscriber.unsubscribe(myTopic, mySubscriberId);
    }

    public void asyncUnsubscribe(final String topic, final String subscriberId, final Callback<Void> callback,
            final Object context) {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = null;
        try {
            mySubscriberId = checkAndConvertSubid(subscriberId);
        } catch (InvalidSubscriberIdException e) {
            callback.operationFailed(context, new ServiceDownException(e));
        }
        subscriber.asyncUnsubscribe(myTopic, mySubscriberId, callback, context);
    }

    public void consume(String topic, String subscriberId, MessageSeqId messageSeqId)
            throws ClientNotSubscribedException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        subscriber.consume(myTopic, mySubscriberId, messageSeqId);
    }

    public boolean hasSubscription(String topic, String subscriberId) throws CouldNotConnectException,
            ServiceDownException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        return subscriber.hasSubscription(myTopic, mySubscriberId);
    }

    // should't be visible to users until it works
    protected List<ByteString> getSubscriptionList(String subscriberId) throws CouldNotConnectException,
            ServiceDownException {
        // Same as the previous hasSubscription method, this data should reside
        // on the server end, not the client side.
        return null;
    }

    public void startDelivery(final String topic, final String subscriberId, final MessageHandler handler)
            throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        subscriber.startDelivery(myTopic, mySubscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // handler.deliver(topic, subscriberId, msg, callback, context);
                handler.deliver(topic, subscriberId, msg, callback, context);
                callback.operationFinished(context, null);
            }

        });
    }

    public void startDeliveryWithFilter(final String topic, final String subscriberId, final MessageHandler handler,
            ClientMessageFilter messageFilter) throws ClientNotSubscribedException, AlreadyStartDeliveryException {

        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        subscriber.startDeliveryWithFilter(myTopic, mySubscriberId, new MessageHandler() {

            @Override
            public void deliver(ByteString topic, ByteString subscriberId, Message msg, Callback<Void> callback,
                    Object context) {
                // handler.deliver(topic, subscriberId, msg, callback,
                // context);
                handler.deliver(topic, subscriberId, msg, callback, context);
                callback.operationFinished(context, null);
            }

        }, messageFilter);
    }

    public void stopDelivery(final String topic, final String subscriberId) throws ClientNotSubscribedException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        subscriber.stopDelivery(myTopic, mySubscriberId);
    }

    public void closeSubscription(String topic, String subscriberId) throws ServiceDownException {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        subscriber.closeSubscription(myTopic, mySubscriberId);
    }

    public void asyncCloseSubscription(final String topic, final String subscriberId, final Callback<Void> callback,
            final Object context) {
        ByteString myTopic = ByteString.copyFromUtf8(topic);
        ByteString mySubscriberId = ByteString.copyFromUtf8(subscriberId);
        subscriber.asyncCloseSubscription(myTopic, mySubscriberId, callback, context);
    }

    // subscriber.addSubscriptionListener() and
    // subscriber.removeSubscriptionListener() is supported inside if needed
    // addSubscriptionListener()
    // removeSubscriptionListener

    private ByteString checkAndConvertSubid(String subscriberId) throws InvalidSubscriberIdException {
        if (subscriberId.equals(SubscriptionStateUtils.QUEUE_SUBID_STR)) {
            throw new InvalidSubscriberIdException("subscriberId in PubSub mode can not be " + subscriberId);
        }
        return ByteString.copyFromUtf8(subscriberId);
    }
}

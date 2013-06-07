package com.yonyou.msgbus.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.InvalidSubscriberIdException;
import org.apache.hedwig.client.netty.HedwigPublisher;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.CouldNotConnectException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PublishResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionOptions;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/***
 * this class contains APIs for messageQueue client, which is fit for the
 * message queue model
 * 
 */
public class MessageQueueClient {

    static final Logger logger = LoggerFactory.getLogger(MessageQueueClient.class);

    private Publisher publisher;
    private Subscriber subscriber;

    // Invisible to users
    MessageQueueClient(Publisher publisher, Subscriber subscriber) {
        this.publisher = publisher;
        this.subscriber = subscriber;
    }

    public boolean createQueue(String queueName) throws InterruptedException {
        final CountDownLatch signal = new CountDownLatch(1);
        final AtomicBoolean bSuccess = new AtomicBoolean(false);
        ((HedwigPublisher) publisher).createQueue(ByteString.copyFromUtf8(queueName), new Callback<ResponseBody>() {

            @Override
            public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
                bSuccess.set(true);
                signal.countDown();
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                bSuccess.set(false);
                signal.countDown();
            }

        }, null);

        if (!signal.await(5, TimeUnit.SECONDS)) {
            logger.info("Can't create queue in 5 seconds.");
            return false;
        }
        return bSuccess.get();
    }

    // ok?
    public boolean deleteQueue(String queueName) throws InterruptedException {
        final CountDownLatch signal = new CountDownLatch(1);
        final AtomicBoolean bSuccess = new AtomicBoolean(false);
        ((HedwigPublisher) publisher).deleteQueue(ByteString.copyFromUtf8(queueName), new Callback<ResponseBody>() {

            @Override
            public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
                bSuccess.set(true);
                signal.countDown();
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                bSuccess.set(false);
                signal.countDown();
            }

        }, null);

        if (!signal.await(5, TimeUnit.SECONDS)) {
            logger.info("Can't delete queue in 5 seconds.");
            return false;
        }
        return bSuccess.get();

    }

    public long queryMessageCount(String queueName) throws InterruptedException {
        final CountDownLatch signal = new CountDownLatch(1);
        final AtomicLong count = new AtomicLong(0);
        ((HedwigPublisher) publisher).getMessageCount(ByteString.copyFromUtf8(queueName), new Callback<ResponseBody>() {

            @Override
            public void operationFinished(Object ctx, ResponseBody resultOfOperation) {
                long result = resultOfOperation.getQueueMgmtResponse().getMessageCount();
                count.set(result);
                signal.countDown();
            }

            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                count.set(-1);
                signal.countDown();
            }

        }, null);

        if (!signal.await(5, TimeUnit.SECONDS)) {
            logger.info("Can't get the count of message for " + queueName + " in 5 seconds.");
            // TODO provide more info
            return -1;
        }
        return count.get();
    }

    public PublishResponse publish(String queueName, String msg) throws CouldNotConnectException, ServiceDownException {
        ByteString topic = ByteString.copyFromUtf8(queueName);
        Message message = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
        return publisher.publish(topic, message);
    }

    public void asyncPublish(String queueName, String msg, Callback<Void> callback, Object context) {
        ByteString topic = ByteString.copyFromUtf8(queueName);
        Message message = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
        publisher.asyncPublish(topic, message, callback, context);
    }

    public void asyncPublishWithResponse(String queueName, String msg, Callback<PublishResponse> callback,
            Object context) {
        ByteString topic = ByteString.copyFromUtf8(queueName);
        Message message = Message.newBuilder().setBody(ByteString.copyFromUtf8(msg)).build();
        publisher.asyncPublishWithResponse(topic, message, callback, context);
    }

    public void consumeMessage(String queueName, MessageSeqId msgSeqId) throws ClientNotSubscribedException {
        subscriber.consume(ByteString.copyFromUtf8(queueName), SubscriptionStateUtils.QUEUE_SUBID_BS, msgSeqId);

    }

    /**
     * @throws AlreadyStartDeliveryException
     * @throws InvalidSubscriberIdException
     * @throws ServiceDownException
     * @throws ClientAlreadySubscribedException
     * @throws CouldNotConnectException
     */
    // In Queue, Put subscribe and delivery together
    public void startDelivery(final String queueName, final MessageHandler handler, SubscriptionOptions options)
            throws ClientNotSubscribedException, AlreadyStartDeliveryException, CouldNotConnectException,
            ClientAlreadySubscribedException, ServiceDownException, InvalidSubscriberIdException {
        subscriber.subscribe(ByteString.copyFromUtf8(queueName), SubscriptionStateUtils.QUEUE_SUBID_BS, options);
        // subscriber.startDelivery(ByteString.copyFromUtf8(queueName),
        // SubscriptionStateUtils.QUEUE_SUBID_BS, handler);
        subscriber.startDelivery(ByteString.copyFromUtf8(queueName), SubscriptionStateUtils.QUEUE_SUBID_BS,
                new MessageHandler() {

                    @Override
                    public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                            Callback<Void> callback, Object context) {
                        // TODO Auto-generated method stub
                        handler.deliver(topic, subscriberId, msg, callback, context);
                        // This is necessary
                        callback.operationFinished(context, null);
                    }

                });
    }

    public void stopDelivery(final String queueName) throws ClientNotSubscribedException {
        subscriber.stopDelivery(ByteString.copyFromUtf8(queueName), SubscriptionStateUtils.QUEUE_SUBID_BS);
    }

    public void closeSubscription(String queueName) throws ServiceDownException {
        subscriber.closeSubscription(ByteString.copyFromUtf8(queueName), SubscriptionStateUtils.QUEUE_SUBID_BS);
    }
}

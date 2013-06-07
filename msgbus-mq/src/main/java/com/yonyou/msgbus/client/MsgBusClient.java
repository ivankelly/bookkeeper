package com.yonyou.msgbus.client;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;

/***
 * this class can get publisher and subscriber from client configuration. also
 * it is used to create a new PubSubClient and MessageQueueClient
 * 
 */
public class MsgBusClient {
    public HedwigClient client;
    public Publisher publisher;
    public Subscriber subscriber;

    private PubSubClient pubSubClient = null;
    private MessageQueueClient mqClient = null;

    public MsgBusClient() {
        ClientConfiguration conf = new ClientConfiguration();
        this.client = new HedwigClient(conf);
        this.publisher = client.getPublisher();
        this.subscriber = client.getSubscriber();
    }

    public MsgBusClient(String localPath) throws MalformedURLException, ConfigurationException {
        this(new URL("file://localhost/" + localPath));
    }

    public MsgBusClient(URL url) throws MalformedURLException, ConfigurationException {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.loadConf(url);
        client = new HedwigClient(cfg);
        this.publisher = client.getPublisher();
        this.subscriber = client.getSubscriber();
    }

    public MsgBusClient(ClientConfiguration cfg) {
        client = new HedwigClient(cfg);
        this.publisher = client.getPublisher();
        this.subscriber = client.getSubscriber();
    }

    public PubSubClient getPubSubClient() {
        if (pubSubClient == null)
            return new PubSubClient(publisher, subscriber);
        else
            return pubSubClient;
    }

    public MessageQueueClient getMessageQueueClient() {
        if (mqClient == null)
            return new MessageQueueClient(publisher, subscriber);
        else
            return mqClient;
    }

    /*
     * public void getAvailableHubs(Callback<ResponseBody> callback) {
     * ((HedwigSubscriber
     * )subscriber).getAvailableHubs(ByteString.copyFromUtf8("q_test"),
     * STATS_ID, callback, null); }
     */

    public void close() {
        client.close();
    }
}

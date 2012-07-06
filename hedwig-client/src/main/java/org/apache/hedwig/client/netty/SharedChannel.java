

/**
 * only used for publish and unsubscribe at the moment
 */
class SharedChannel {
    Channel channel;
    enum State {
        NOTCONNECTED,
        CONNECTING,
        CONNECTED
    };
    State state = State.NOTCONNECTED;

    void submitOp(PubSubData op) {
        if (channel.isConnected()
            && queue.isEmpty()) {
            doOp(op);
        } else {
            queue.push(op);
            connect();
        }
    }

    void onConnected() {
        synchronized (state) {
            state = State.CONNECTED;
        }
        for (PubSubData op : queue) {
            doOp(op);
        }
    }

    void onError() {
    }

    private void connect() {
        synchronized (state) {
            if (state == State.CONNECTING
                && state == State.CONNECTED) {
                return;
            }
            state = State.CONNECTING;
        }
        
    }

    
    
}

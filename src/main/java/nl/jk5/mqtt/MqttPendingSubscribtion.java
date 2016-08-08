package nl.jk5.mqtt;

import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.util.concurrent.Promise;

import java.util.HashSet;
import java.util.Set;

final class MqttPendingSubscribtion {

    private final int messageId;
    private final Promise<Void> future;
    private final String topic;
    private final MqttSubscribeMessage subscribeMessage;
    private final Set<MqttPendingHandler> handlers = new HashSet<>();

    private boolean sent = false;

    public MqttPendingSubscribtion(int messageId, Promise<Void> future, String topic, MqttSubscribeMessage message) {
        this.messageId = messageId;
        this.future = future;
        this.topic = topic;
        this.subscribeMessage = message;
    }

    public int getMessageId() {
        return messageId;
    }

    public Promise<Void> getFuture() {
        return future;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isSent() {
        return sent;
    }

    public void setSent(boolean sent) {
        this.sent = sent;
    }

    public MqttSubscribeMessage getSubscribeMessage() {
        return subscribeMessage;
    }

    public void addHandler(MqttHandler handler, boolean once){
        this.handlers.add(new MqttPendingHandler(handler, once));
    }

    public Set<MqttPendingHandler> getHandlers() {
        return handlers;
    }

    final class MqttPendingHandler {
        private final MqttHandler handler;
        private final boolean once;

        public MqttPendingHandler(MqttHandler handler, boolean once) {
            this.handler = handler;
            this.once = once;
        }

        public MqttHandler getHandler() {
            return handler;
        }

        public boolean isOnce() {
            return once;
        }
    }
}

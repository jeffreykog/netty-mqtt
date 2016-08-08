package nl.jk5.mqtt;

import io.netty.util.concurrent.Promise;

final class MqttPendingUnsubscribtion {

    private final int messageId;
    private final Promise<Void> future;
    private final String topic;

    public MqttPendingUnsubscribtion(int messageId, Promise<Void> future, String topic) {
        this.messageId = messageId;
        this.future = future;
        this.topic = topic;
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
}

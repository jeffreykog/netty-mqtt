package nl.jk5.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Promise;

final class MqttPendingPublish {

    private final int messageId;
    private final Promise<Void> future;
    private final String topic;
    private final ByteBuf payload;
    private final MqttPublishMessage message;
    private final MqttQoS qos;

    private boolean sent = false;

    public MqttPendingPublish(int messageId, Promise<Void> future, String topic, ByteBuf payload, MqttPublishMessage message, MqttQoS qos) {
        this.messageId = messageId;
        this.future = future;
        this.topic = topic;
        this.payload = payload;
        this.message = message;
        this.qos = qos;
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

    public ByteBuf getPayload() {
        return payload;
    }

    public boolean isSent() {
        return sent;
    }

    public void setSent(boolean sent) {
        this.sent = sent;
    }

    public MqttPublishMessage getMessage() {
        return message;
    }

    public MqttQoS getQos() {
        return qos;
    }
}

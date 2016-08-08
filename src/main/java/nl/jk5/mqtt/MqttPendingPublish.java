package nl.jk5.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Promise;

import java.util.function.Consumer;

final class MqttPendingPublish {

    private final int messageId;
    private final Promise<Void> future;
    private final ByteBuf payload;
    private final MqttPublishMessage message;
    private final MqttQoS qos;

    private final RetransmissionHandler<MqttPublishMessage> publishRetransmissionHandler = new RetransmissionHandler<>();
    private final RetransmissionHandler<MqttMessage> pubrelRetransmissionHandler = new RetransmissionHandler<>();

    private boolean sent = false;

    public MqttPendingPublish(int messageId, Promise<Void> future, ByteBuf payload, MqttPublishMessage message, MqttQoS qos) {
        this.messageId = messageId;
        this.future = future;
        this.payload = payload;
        this.message = message;
        this.qos = qos;

        this.publishRetransmissionHandler.setOriginalMessage(message);
    }

    public int getMessageId() {
        return messageId;
    }

    public Promise<Void> getFuture() {
        return future;
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

    public void startPublishRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.publishRetransmissionHandler.setHandle(((fixedHeader, originalMessage) -> {
            sendPacket.accept(new MqttPublishMessage(fixedHeader, originalMessage.variableHeader(), this.payload.retain()));
        }));
        this.publishRetransmissionHandler.start(eventLoop);
    }

    public void onPubackReceived() {
        this.publishRetransmissionHandler.stop();
    }

    public void setPubrelMessage(MqttMessage pubrelMessage) {
        this.pubrelRetransmissionHandler.setOriginalMessage(pubrelMessage);
    }

    public void startPubrelRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.pubrelRetransmissionHandler.setHandle((fixedHeader, originalMessage) -> {
            sendPacket.accept(new MqttMessage(fixedHeader, originalMessage.variableHeader()));
        });
        this.pubrelRetransmissionHandler.start(eventLoop);
    }

    public void onPubcompReceived() {
        this.pubrelRetransmissionHandler.stop();
    }
}

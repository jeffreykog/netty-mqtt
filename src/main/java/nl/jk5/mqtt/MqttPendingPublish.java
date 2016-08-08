package nl.jk5.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class MqttPendingPublish {

    private final int messageId;
    private final Promise<Void> future;
    private final String topic;
    private final ByteBuf payload;
    private final MqttPublishMessage message;
    private final MqttQoS qos;

    private ScheduledFuture<?> publishRetransmissionTimer;
    private ScheduledFuture<?> pubrelRetransmissionTimer;
    private int publishRetransmitTimeout = 10;
    private int pubrelRetransmitTimeout = 10;

    private boolean sent = false;
    private MqttMessage pubrelMessage;

    public MqttPendingPublish(int messageId, Promise<Void> future, String topic, ByteBuf payload, MqttPublishMessage message, MqttQoS qos) {
        this.messageId = messageId;
        this.future = future;
        this.topic = topic;
        this.payload = payload.retain();
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

    public void startPublishRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.publishRetransmissionTimer = eventLoop.schedule(() -> {
            this.publishRetransmitTimeout += 5;

            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, true, qos, this.message.fixedHeader().isRetain(), 0);
            MqttPublishMessage message = new MqttPublishMessage(fixedHeader, this.message.variableHeader(), this.payload.retain());

            sendPacket.accept(message);
            startPublishRetransmissionTimer(eventLoop, sendPacket);
        }, publishRetransmitTimeout, TimeUnit.SECONDS);
    }

    public void onPubackReceived() {
        if(this.publishRetransmissionTimer != null){
            this.publishRetransmissionTimer.cancel(true);
        }
    }

    public void setPubrelMessage(MqttMessage pubrelMessage) {
        this.pubrelMessage = pubrelMessage;
    }

    public void startPubrelRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.pubrelRetransmissionTimer = eventLoop.schedule(() -> {
            this.pubrelRetransmitTimeout += 5;

            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, true, MqttQoS.AT_LEAST_ONCE, false, 0);
            MqttMessage pubrelMessage = new MqttMessage(fixedHeader, this.pubrelMessage.variableHeader());

            sendPacket.accept(pubrelMessage);
            startPubrelRetransmissionTimer(eventLoop, sendPacket);
        }, pubrelRetransmitTimeout, TimeUnit.SECONDS);
    }

    public void onPubcompReceived() {
        if(this.pubrelRetransmissionTimer != null){
            this.pubrelRetransmissionTimer.cancel(true);
        }
    }
}

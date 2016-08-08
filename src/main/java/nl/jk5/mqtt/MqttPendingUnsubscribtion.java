package nl.jk5.mqtt;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class MqttPendingUnsubscribtion {

    private final int messageId;
    private final Promise<Void> future;
    private final String topic;
    private final MqttUnsubscribeMessage unsubscribeMessage;

    private ScheduledFuture<?> retransmissionTimer;
    private int retransmitTimeout = 10;

    public MqttPendingUnsubscribtion(int messageId, Promise<Void> future, String topic, MqttUnsubscribeMessage unsubscribeMessage) {
        this.messageId = messageId;
        this.future = future;
        this.topic = topic;
        this.unsubscribeMessage = unsubscribeMessage;
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

    public void startRetransmissionTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.retransmissionTimer = eventLoop.schedule(() -> {
            this.retransmitTimeout += 5;

            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
            MqttUnsubscribeMessage message = new MqttUnsubscribeMessage(fixedHeader, this.unsubscribeMessage.variableHeader(), this.unsubscribeMessage.payload());

            sendPacket.accept(message);
            startRetransmissionTimer(eventLoop, sendPacket);
        }, retransmitTimeout, TimeUnit.SECONDS);
    }

    public void onUnsubackReceived(){
        if(this.retransmissionTimer != null){
            this.retransmissionTimer.cancel(true);
        }
    }
}

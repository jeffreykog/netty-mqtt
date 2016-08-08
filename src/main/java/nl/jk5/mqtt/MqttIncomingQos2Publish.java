package nl.jk5.mqtt;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class MqttIncomingQos2Publish {

    private final MqttPublishMessage message;
    private final MqttMessage pubrecMessage;

    private ScheduledFuture<?> pubrelTimer;
    private int retransmitTimeout = 10;

    public MqttIncomingQos2Publish(MqttPublishMessage message, MqttMessage pubrecMessage) {
        this.message = message;
        this.pubrecMessage = pubrecMessage;
    }

    public MqttPublishMessage getMessage() {
        return message;
    }

    public void startPubrelRetransmitTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.pubrelTimer = eventLoop.schedule(() -> {
            this.retransmitTimeout += 5;
            MqttFixedHeader fixedHeader1 = new MqttFixedHeader(MqttMessageType.PUBREC, true, MqttQoS.AT_LEAST_ONCE, false, 0);
            MqttMessage msg2 = new MqttMessage(fixedHeader1, pubrecMessage.variableHeader());
            sendPacket.accept(msg2);
        }, retransmitTimeout, TimeUnit.SECONDS);
    }

    public void onPubrelReceived() {
        if(this.pubrelTimer != null){
            this.pubrelTimer.cancel(true);
        }
    }
}

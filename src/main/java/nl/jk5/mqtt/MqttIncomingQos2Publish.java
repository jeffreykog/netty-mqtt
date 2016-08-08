package nl.jk5.mqtt;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.*;

import java.util.function.Consumer;

final class MqttIncomingQos2Publish {

    private final MqttPublishMessage incomingPublish;

    private final RetransmissionHandler<MqttMessage> retransmissionHandler = new RetransmissionHandler<>();

    public MqttIncomingQos2Publish(MqttPublishMessage incomingPublish, MqttMessage originalMessage) {
        this.incomingPublish = incomingPublish;

        this.retransmissionHandler.setOriginalMessage(originalMessage);
    }

    public MqttPublishMessage getIncomingPublish() {
        return incomingPublish;
    }

    public void startPubrecRetransmitTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        this.retransmissionHandler.setHandle((fixedHeader, originalMessage) -> {
            sendPacket.accept(new MqttMessage(fixedHeader, originalMessage.variableHeader()));
        });
        this.retransmissionHandler.start(eventLoop);
    }

    public void onPubrelReceived() {
        this.retransmissionHandler.stop();
    }
}

package nl.jk5.mqtt;

import io.netty.channel.EventLoop;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

final class MqttPendingSubscribtion {

    private final int messageId;
    private final Promise<Void> future;
    private final String topic;
    private final MqttSubscribeMessage subscribeMessage;
    private final Set<MqttPendingHandler> handlers = new HashSet<>();

    private ScheduledFuture<?> retransmitTimer;
    private boolean sent = false;
    private int retransmitTimeout = 15;

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

    public ScheduledFuture<?> getRetransmitTimer() {
        return retransmitTimer;
    }

    public void setRetransmitTimer(ScheduledFuture<?> retransmitTimer) {
        this.retransmitTimer = retransmitTimer;
    }

    public int getRetransmitTimeout() {
        return retransmitTimeout;
    }

    public void setRetransmitTimeout(int retransmitTimeout) {
        this.retransmitTimeout = retransmitTimeout;
    }

    public void startRetransmitTimer(EventLoop eventLoop, Consumer<Object> sendPacket) {
        if(this.sent){ //If the packet is sent successfully, we can start the retransmit timer
            this.setRetransmitTimer(eventLoop.schedule(() -> {
                this.setRetransmitTimeout(this.getRetransmitTimeout() + 5);
                MqttFixedHeader fixedHeader1 = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, true, MqttQoS.AT_LEAST_ONCE, false, 0);
                MqttSubscribeMessage msg2 = new MqttSubscribeMessage(fixedHeader1, this.getSubscribeMessage().variableHeader(), this.getSubscribeMessage().payload());
                sendPacket.accept(msg2);
                startRetransmitTimer(eventLoop, sendPacket);
            }, this.getRetransmitTimeout(), TimeUnit.SECONDS));
        }
    }

    public void onSubackReceived(){
        if(this.retransmitTimer != null){
            this.retransmitTimer.cancel(true);
        }
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

package nl.jk5.mqtt;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.concurrent.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

final class MqttChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private static final Logger logger = LogManager.getLogger();

    private final MqttClient client;
    private final Promise<MqttConnectResult> connectFuture;

    MqttChannelHandler(MqttClient client, Promise<MqttConnectResult> connectFuture) {
        this.client = client;
        this.connectFuture = connectFuture;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        logger.info(msg);
        switch (msg.fixedHeader().messageType()){
            case CONNACK:
                handleConack(ctx, (MqttConnAckMessage) msg);
                break;
            case SUBACK:
                handleSubAck(ctx, (MqttSubAckMessage) msg);
                break;
            case PUBLISH:
                handlePublish(ctx, (MqttPublishMessage) msg);
                break;
            case UNSUBACK:
                handleUnsuback(ctx, (MqttUnsubAckMessage) msg);
                break;
            case PUBACK:
                handlePuback(ctx, (MqttPubAckMessage) msg);
                break;
            case PUBREC:
                handlePubrec(ctx, msg);
                break;
            case PUBREL:
                handlePubrel(ctx, msg);
                break;
            case PUBCOMP:
                handlePubcomp(ctx, msg);
                break;
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        this.client.setChannel(ctx.channel());

        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNECT, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(
                this.client.getClientConfig().getProtocolVersion().protocolName(),  // Protocol Name
                this.client.getClientConfig().getProtocolVersion().protocolLevel(), // Protocol Level
                this.client.getClientConfig().getUsername() != null,                // Has Username
                this.client.getClientConfig().getPassword() != null,                // Has Password
                this.client.getClientConfig().getLastWill() != null                 // Will Retain
                        && this.client.getClientConfig().getLastWill().isRetain(),
                this.client.getClientConfig().getLastWill() != null                 // Will QOS
                        ? this.client.getClientConfig().getLastWill().getQos().value()
                        : 0,
                this.client.getClientConfig().getLastWill() != null,                // Has Will
                this.client.getClientConfig().isCleanSession(),                     // Clean Session
                this.client.getClientConfig().getTimeoutSeconds()                   // Timeout
        );
        MqttConnectPayload payload = new MqttConnectPayload(
                this.client.getClientConfig().getClientId(),
                this.client.getClientConfig().getLastWill() != null ? this.client.getClientConfig().getLastWill().getTopic() : null,
                this.client.getClientConfig().getLastWill() != null ? this.client.getClientConfig().getLastWill().getMessage() : null,
                this.client.getClientConfig().getUsername(),
                this.client.getClientConfig().getPassword()
        );
        this.sendAndFlushPacket(new MqttConnectMessage(fixedHeader, variableHeader, payload));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    private ChannelFuture sendAndFlushPacket(Object message){
        return this.client.sendAndFlushPacket(message);
    }

    private void invokeHandlersForIncomingPublish(MqttPublishMessage message){
        Set<MqttSubscribtion> subscribtions = ImmutableSet.copyOf(this.client.getSubscriptions().get(message.variableHeader().topicName()));
        for (MqttSubscribtion subscribtion : subscribtions) {
            if(subscribtion.isOnce() && subscribtion.isCalled()){
                continue;
            }
            message.payload().markReaderIndex();
            subscribtion.setCalled(true);
            subscribtion.getHandler().onMessage(message.variableHeader().topicName(), message.payload());
            if(subscribtion.isOnce()){
                this.client.off(subscribtion.getTopic(), subscribtion.getHandler());
            }
            message.payload().resetReaderIndex();
        }
    }

    private void handleConack(ChannelHandlerContext ctx, MqttConnAckMessage message){
        switch(message.variableHeader().connectReturnCode()){
            case CONNECTION_ACCEPTED:
                this.connectFuture.setSuccess(new MqttConnectResult(true, ctx.channel().closeFuture()));

                this.client.getPendingSubscribtions().entrySet().stream().filter((e) -> !e.getValue().isSent()).forEach((e) -> {
                    this.sendAndFlushPacket(e.getValue().getSubscribeMessage());
                    e.getValue().setSent(true);
                });

                this.client.getPendingPublishes().forEach((id, publish) -> {
                    if(publish.isSent()) return;
                    this.sendAndFlushPacket(publish.getMessage());
                    publish.setSent(true);
                    if(publish.getQos() == MqttQoS.AT_MOST_ONCE){
                        publish.getFuture().setSuccess(null); //We don't get an ACK for QOS 0
                        this.client.getPendingPublishes().remove(publish.getMessageId());
                    }
                });
        }
    }

    private void handleSubAck(ChannelHandlerContext ctx, MqttSubAckMessage message){
        MqttPendingSubscribtion pendingSubscribtion = this.client.getPendingSubscribtions().get(message.variableHeader().messageId());
        if(pendingSubscribtion == null){
            return;
        }
        pendingSubscribtion.onSubackReceived();
        for (MqttPendingSubscribtion.MqttPendingHandler handler : pendingSubscribtion.getHandlers()) {
            MqttSubscribtion subscribtion = new MqttSubscribtion(pendingSubscribtion.getTopic(), handler.getHandler(), handler.isOnce());
            this.client.getSubscriptions().put(pendingSubscribtion.getTopic(), subscribtion);
            this.client.getHandlerToSubscribtion().put(handler.getHandler(), subscribtion);
        }
        this.client.getPendingSubscribeTopics().remove(pendingSubscribtion.getTopic());

        this.client.getServerSubscribtions().add(pendingSubscribtion.getTopic());
        pendingSubscribtion.getFuture().setSuccess(null);
    }

    private void handlePublish(ChannelHandlerContext ctx, MqttPublishMessage message){
        switch (message.fixedHeader().qosLevel()){
            case AT_MOST_ONCE:
                invokeHandlersForIncomingPublish(message);
                break;

            case AT_LEAST_ONCE:
                invokeHandlersForIncomingPublish(message);
                if(message.variableHeader().messageId() != -1){
                    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());
                    this.sendAndFlushPacket(new MqttPubAckMessage(fixedHeader, variableHeader));
                }
                break;

            case EXACTLY_ONCE:
                if(message.variableHeader().messageId() != -1){
                    MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
                    MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());
                    MqttMessage pubrecMessage = new MqttMessage(fixedHeader, variableHeader);

                    MqttIncomingQos2Publish incomingQos2Publish = new MqttIncomingQos2Publish(message, pubrecMessage);
                    this.client.getQos2PendingIncomingPublishes().put(message.variableHeader().messageId(), incomingQos2Publish);
                    incomingQos2Publish.startPubrelRetransmitTimer(this.client.getEventLoop().next(), this.client::sendAndFlushPacket);

                    this.sendAndFlushPacket(pubrecMessage);
                }
                break;
        }
    }

    private void handleUnsuback(ChannelHandlerContext ctx, MqttUnsubAckMessage message){
        MqttPendingUnsubscribtion unsubscribtion = this.client.getPendingServerUnsubscribes().get(message.variableHeader().messageId());
        if(unsubscribtion == null){
            return;
        }
        this.client.getServerSubscribtions().remove(unsubscribtion.getTopic());
        unsubscribtion.getFuture().setSuccess(null);
        this.client.getPendingServerUnsubscribes().remove(message.variableHeader().messageId());
    }

    //TODO: RETRANSMIT PUBLISH: retry sending the packet untill we receive an PUBACK
    private void handlePuback(ChannelHandlerContext ctx, MqttPubAckMessage message){
        MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(message.variableHeader().messageId());
        pendingPublish.getFuture().setSuccess(null);
        this.client.getPendingPublishes().remove(message.variableHeader().messageId());
    }

    //TODO: RETRANSMIT PUBREL: retry sending the packet untill we receive an PUBREC
    //TODO: RETRANSMIT PUBREL: mosquitto always sends dup=false
    private void handlePubrec(ChannelHandlerContext ctx, MqttMessage message){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        this.sendAndFlushPacket(new MqttMessage(fixedHeader, variableHeader));
    }

    private void handlePubrel(ChannelHandlerContext ctx, MqttMessage message){
        if(this.client.getQos2PendingIncomingPublishes().containsKey(((MqttMessageIdVariableHeader) message.variableHeader()).messageId())){
            MqttIncomingQos2Publish incomingQos2Publish = this.client.getQos2PendingIncomingPublishes().get(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
            this.invokeHandlersForIncomingPublish(incomingQos2Publish.getMessage());
            incomingQos2Publish.onPubrelReceived();
            this.client.getQos2PendingIncomingPublishes().remove(incomingQos2Publish.getMessage().variableHeader().messageId());
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(((MqttMessageIdVariableHeader) message.variableHeader()).messageId());
        this.sendAndFlushPacket(new MqttMessage(fixedHeader, variableHeader));
    }

    private void handlePubcomp(ChannelHandlerContext ctx, MqttMessage message){
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(variableHeader.messageId());
        pendingPublish.getFuture().setSuccess(null);
        this.client.getPendingPublishes().remove(variableHeader.messageId());
    }
}

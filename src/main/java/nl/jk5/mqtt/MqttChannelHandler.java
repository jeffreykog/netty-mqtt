package nl.jk5.mqtt;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import io.netty.util.concurrent.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Random;
import java.util.Set;

class MqttChannelHandler extends SimpleChannelInboundHandler<MqttMessage> {

    static final int KEEPALIVE_SECONDS = 10;
    private static final MqttVersion PROTOCOL_VERSION = MqttVersion.MQTT_3_1;

    private static final Logger logger = LogManager.getLogger();

    private final MqttClient client;
    private final Promise<MqttConnectResult> connectFuture;

    private final String clientId;

    MqttChannelHandler(MqttClient client, Promise<MqttConnectResult> connectFuture) {
        this.client = client;
        this.connectFuture = connectFuture;

        Random random = new Random();
        String id = "omni/";
        String[] options = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".split("");
        for(int i = 0; i < 8; i++){
            id += options[random.nextInt(options.length)];
        }
        this.clientId = id;
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
        MqttConnectVariableHeader variableHeader = new MqttConnectVariableHeader(PROTOCOL_VERSION.protocolName(), PROTOCOL_VERSION.protocolLevel(), false, false, false, 0, false, false, KEEPALIVE_SECONDS);
        MqttConnectPayload payload = new MqttConnectPayload(clientId, null, null, null, null);
        this.sendAndFlushPacket(new MqttConnectMessage(fixedHeader, variableHeader, payload));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    private ChannelFuture sendAndFlushPacket(Object message){
        return this.client.sendAndFlushPacket(message);
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
        if(message.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE && message.variableHeader().messageId() != -1){
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());
            this.sendAndFlushPacket(new MqttPubAckMessage(fixedHeader, variableHeader));
        }
        if(message.fixedHeader().qosLevel() == MqttQoS.EXACTLY_ONCE && message.variableHeader().messageId() != -1){
            //TODO: keep spamming PUBREC until we receive PUBREL
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(message.variableHeader().messageId());
            this.sendAndFlushPacket(new MqttMessage(fixedHeader, variableHeader));
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

    //TODO: retry sending the packet untill we receive an PUBACK
    private void handlePuback(ChannelHandlerContext ctx, MqttPubAckMessage message){
        MqttPendingPublish pendingPublish = this.client.getPendingPublishes().get(message.variableHeader().messageId());
        pendingPublish.getFuture().setSuccess(null);
        this.client.getPendingPublishes().remove(message.variableHeader().messageId());
    }

    //TODO: retry sending the packet untill we receive an PUBREC
    private void handlePubrec(ChannelHandlerContext ctx, MqttMessage message){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        this.sendAndFlushPacket(new MqttMessage(fixedHeader, variableHeader));
    }

    private void handlePubrel(ChannelHandlerContext ctx, MqttMessage message){
        //When a subscriber receives a PUBREL message from the server, the subscriber makes the message available to the subscribing application and sends a PUBCOMP message to the server.
        //TODO: wait with executing handlers untill this point.
        //TODO: Save if we executed the handlers. If we did it once, don't do it again, but do re-send a PUBCOMP
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

    private void sendKeepAlive(){
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
        this.sendAndFlushPacket(new MqttMessage(fixedHeader));
    }
}

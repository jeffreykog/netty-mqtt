package nl.jk5.mqtt;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MqttClient {

    private final Set<String> serverSubscribtions = new HashSet<>();
    private final IntObjectHashMap<MqttPendingUnsubscribtion> pendingServerUnsubscribes = new IntObjectHashMap<>();
    private final IntObjectHashMap<MqttPendingPublish> pendingPublishes = new IntObjectHashMap<>();
    private final HashMultimap<String, MqttSubscribtion> subscriptions = HashMultimap.create();
    private final IntObjectHashMap<MqttPendingSubscribtion> pendingSubscribtions = new IntObjectHashMap<>();
    private final Set<String> pendingSubscribeTopics = new HashSet<>();
    private final HashMultimap<MqttHandler, MqttSubscribtion> handlerToSubscribtion = HashMultimap.create();
    private final AtomicInteger nextMessageId = new AtomicInteger(1);

    private EventLoopGroup eventLoop;

    private Channel channel;

    public MqttClient(){

    }

    public Future<MqttConnectResult> connect(String host){
        return connect(host, 1883);
    }

    public Future<MqttConnectResult> connect(String host, int port){
        if(this.eventLoop == null){
            if(Epoll.isAvailable()){
                this.eventLoop = new EpollEventLoopGroup();
            }else{
                this.eventLoop = new NioEventLoopGroup();
            }
        }
        Promise<MqttConnectResult> connectFuture = new DefaultPromise<MqttConnectResult>(this.eventLoop.next());
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoop);
        if(this.eventLoop instanceof EpollEventLoopGroup){
            bootstrap.channel(EpollSocketChannel.class);
        }else if(this.eventLoop instanceof NioEventLoopGroup){
            bootstrap.channel(NioSocketChannel.class);
        }
        bootstrap.remoteAddress(host, port);
        bootstrap.handler(new MqttChannelInitializer(connectFuture));
        ChannelFuture future = bootstrap.connect();
        future.addListener((ChannelFutureListener) f -> {
            MqttClient.this.channel = f.channel();
        });

        return connectFuture;
    }

    public EventLoopGroup getEventLoop() {
        return eventLoop;
    }

    public void setEventLoop(EventLoopGroup eventLoop) {
        this.eventLoop = eventLoop;
    }

    Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public Future<Void> on(String topic, MqttHandler handler) {
        return on(topic, handler, MqttQoS.AT_MOST_ONCE);
    }

    public Future<Void> on(String topic, MqttHandler handler, MqttQoS qos) {
        return createSubscribtion(topic, handler, false, qos);
    }

    public Future<Void> once(String topic, MqttHandler handler) {
        return once(topic, handler, MqttQoS.AT_MOST_ONCE);
    }

    public Future<Void> once(String topic, MqttHandler handler, MqttQoS qos) {
        return createSubscribtion(topic, handler, true, qos);
    }

    public Future<Void> off(String topic, MqttHandler handler) {
        Promise<Void> future = new DefaultPromise<>(this.eventLoop.next());
        for (MqttSubscribtion subscribtion : this.handlerToSubscribtion.get(handler)) {
            this.subscriptions.remove(topic, subscribtion);
        }
        this.handlerToSubscribtion.removeAll(handler);
        this.checkSubscribtions(topic, future);
        return future;
    }

    public Future<Void> off(String topic) {
        Promise<Void> future = new DefaultPromise<>(this.eventLoop.next());
        ImmutableSet<MqttSubscribtion> subscribtions = ImmutableSet.copyOf(this.subscriptions.get(topic));
        for (MqttSubscribtion subscribtion : subscribtions) {
            for (MqttSubscribtion handSub : this.handlerToSubscribtion.get(subscribtion.getHandler())) {
                this.subscriptions.remove(topic, handSub);
            }
            this.handlerToSubscribtion.remove(subscribtion.getHandler(), subscribtion);
        }
        this.checkSubscribtions(topic, future);
        return future;
    }

    public Future<Void> publish(String topic, ByteBuf payload){
        return publish(topic, payload, MqttQoS.AT_MOST_ONCE);
    }

    public Future<Void> publish(String topic, ByteBuf payload, MqttQoS qos){
        Promise<Void> future = new DefaultPromise<>(this.eventLoop.next());
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topic, getNewMessageId().messageId());
        MqttPublishMessage message = new MqttPublishMessage(fixedHeader, variableHeader, payload);

        MqttPendingPublish pendingPublish = new MqttPendingPublish(variableHeader.messageId(), future, topic, payload, message, qos);
        pendingPublish.setSent(this.sendAndFlushPacket(message) != null);

        if(pendingPublish.isSent() && pendingPublish.getQos() == MqttQoS.AT_MOST_ONCE){
            pendingPublish.getFuture().setSuccess(null); //We don't get an ACK for QOS 0
        }else{
            this.pendingPublishes.put(pendingPublish.getMessageId(), pendingPublish);
        }

        return future;
    }

    ChannelFuture sendAndFlushPacket(Object message){
        if(this.channel == null){
            return null;
        }
        if(this.channel.isActive()){
            return this.channel.writeAndFlush(message);
        }
        return this.channel.newFailedFuture(new RuntimeException("Channel is closed"));
    }

    MqttMessageIdVariableHeader getNewMessageId(){
        this.nextMessageId.compareAndSet(0xffff, 1);
        return MqttMessageIdVariableHeader.from(this.nextMessageId.getAndIncrement());
    }

    //TODO: allow to subscribe on an higher QOS, send the proper PUBACK when we receive a message
    //TODO: Do this based on the granted QOS level in the SUBACK packet
    Future<Void> createSubscribtion(String topic, MqttHandler handler, boolean once, MqttQoS qos){
        if(this.pendingSubscribeTopics.contains(topic)){
            Optional<Map.Entry<Integer, MqttPendingSubscribtion>> subscribtionEntry = this.pendingSubscribtions.entrySet().stream().filter((e) -> e.getValue().getTopic().equals(topic)).findAny();
            if(subscribtionEntry.isPresent()){
                subscribtionEntry.get().getValue().addHandler(handler, once);
                return subscribtionEntry.get().getValue().getFuture();
            }
        }
        if(this.serverSubscribtions.contains(topic)){
            MqttSubscribtion subscribtion = new MqttSubscribtion(topic, handler, once);
            this.subscriptions.put(topic, subscribtion);
            this.handlerToSubscribtion.put(handler, subscribtion);
            return this.channel.newSucceededFuture();
        }

        //TODO: when no suback received, keep retransmitting the SUBSCRIBE message
        Promise<Void> future = new DefaultPromise<>(this.eventLoop.next());
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
        MqttTopicSubscription subscription = new MqttTopicSubscription(topic, qos);
        MqttMessageIdVariableHeader variableHeader = getNewMessageId();
        MqttSubscribePayload payload = new MqttSubscribePayload(Collections.singletonList(subscription));
        MqttSubscribeMessage message = new MqttSubscribeMessage(fixedHeader, variableHeader, payload);

        MqttPendingSubscribtion pendingSubscribtion = new MqttPendingSubscribtion(variableHeader.messageId(), future, topic, message);
        pendingSubscribtion.addHandler(handler, once);
        this.pendingSubscribtions.put(variableHeader.messageId(), pendingSubscribtion);
        this.pendingSubscribeTopics.add(topic);
        pendingSubscribtion.setSent(this.sendAndFlushPacket(message) != null);
        return future;
    }

    void checkSubscribtions(String topic, Promise<Void> promise){
        if(!(this.subscriptions.containsKey(topic) && this.subscriptions.get(topic).size() != 0) && this.serverSubscribtions.contains(topic)){
            //TODO: retry sending when no ACK
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBSCRIBE, false, MqttQoS.AT_LEAST_ONCE, false, 0);
            MqttMessageIdVariableHeader variableHeader = getNewMessageId();
            MqttUnsubscribePayload payload = new MqttUnsubscribePayload(Collections.singletonList(topic));
            MqttUnsubscribeMessage message = new MqttUnsubscribeMessage(fixedHeader, variableHeader, payload);
            this.pendingServerUnsubscribes.put(variableHeader.messageId(), new MqttPendingUnsubscribtion(variableHeader.messageId(), promise, topic));
            this.sendAndFlushPacket(message);
        }else{
            promise.setSuccess(null);
        }
    }

    IntObjectHashMap<MqttPendingSubscribtion> getPendingSubscribtions() {
        return pendingSubscribtions;
    }

    HashMultimap<String, MqttSubscribtion> getSubscriptions() {
        return subscriptions;
    }

    Set<String> getPendingSubscribeTopics() {
        return pendingSubscribeTopics;
    }

    HashMultimap<MqttHandler, MqttSubscribtion> getHandlerToSubscribtion() {
        return handlerToSubscribtion;
    }

    Set<String> getServerSubscribtions() {
        return serverSubscribtions;
    }

    IntObjectHashMap<MqttPendingUnsubscribtion> getPendingServerUnsubscribes() {
        return pendingServerUnsubscribes;
    }

    IntObjectHashMap<MqttPendingPublish> getPendingPublishes() {
        return pendingPublishes;
    }

    private class MqttChannelInitializer extends ChannelInitializer<SocketChannel> {

        private final Promise<MqttConnectResult> connectFuture;

        MqttChannelInitializer(Promise<MqttConnectResult> connectFuture) {
            this.connectFuture = connectFuture;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast("mqttDecoder", new MqttDecoder());
            ch.pipeline().addLast("mqttEncoder", MqttEncoder.INSTANCE);
            ch.pipeline().addLast("idleStateHandler", new IdleStateHandler(MqttChannelHandler.KEEPALIVE_SECONDS + 5, MqttChannelHandler.KEEPALIVE_SECONDS, 0));
            ch.pipeline().addLast("mqttPingHandler", new MqttPingHandler(MqttChannelHandler.KEEPALIVE_SECONDS));
            ch.pipeline().addLast("mqttHandler", new MqttChannelHandler(MqttClient.this, connectFuture));
        }
    }
}

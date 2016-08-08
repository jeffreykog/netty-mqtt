package nl.jk5.mqtt;

import io.netty.channel.ChannelFuture;

public final class MqttConnectResult {

    private final boolean success;
    private final ChannelFuture closeFuture;

    MqttConnectResult(boolean success, ChannelFuture closeFuture) {
        this.success = success;
        this.closeFuture = closeFuture;
    }

    public boolean isSuccess() {
        return success;
    }

    public ChannelFuture getCloseFuture() {
        return closeFuture;
    }
}

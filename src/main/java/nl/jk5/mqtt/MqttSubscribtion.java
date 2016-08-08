package nl.jk5.mqtt;

import javax.annotation.Nonnull;

final class MqttSubscribtion {

    @Nonnull
    private final String topic;

    @Nonnull
    private final MqttHandler handler;

    private final boolean once;
    private boolean called;

    public MqttSubscribtion(@Nonnull String topic, @Nonnull MqttHandler handler, boolean once) {
        if(topic == null){
            throw new NullPointerException("topic");
        }
        if(handler == null){
            throw new NullPointerException("handler");
        }
        this.topic = topic;
        this.handler = handler;
        this.once = once;
    }

    @Nonnull
    public String getTopic() {
        return topic;
    }

    @Nonnull
    public MqttHandler getHandler() {
        return handler;
    }

    @Nonnull
    public boolean isOnce() {
        return once;
    }

    public boolean isCalled() {
        return called;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MqttSubscribtion that = (MqttSubscribtion) o;

        if (once != that.once) return false;
        if (!topic.equals(that.topic)) return false;
        return handler.equals(that.handler);

    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + handler.hashCode();
        result = 31 * result + (once ? 1 : 0);
        return result;
    }

    public void setCalled(boolean called) {
        this.called = called;
    }
}

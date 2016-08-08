package nl.jk5.mqtt.test;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;
import nl.jk5.mqtt.MqttClient;

public class MqttTest {

    public static void main(String[] args) {
        MqttClient client = new MqttClient();
        client.getClientConfig();
        client.connect("10.2.6.11");
        client.on("test/+/info", (topic, payload) ->
                System.out.println(topic + ": " + payload.toString(CharsetUtil.UTF_8))
            , MqttQoS.EXACTLY_ONCE);
        client.on("testinfo/something/#", (topic, payload) ->
                System.out.println(topic + ": " + payload.toString(CharsetUtil.UTF_8))
            , MqttQoS.EXACTLY_ONCE);
    }
}

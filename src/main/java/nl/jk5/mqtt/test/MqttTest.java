package nl.jk5.mqtt.test;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;
import nl.jk5.mqtt.MqttClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MqttTest {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        MqttClient client = new MqttClient();
        client.getClientConfig();
        client.connect("10.2.6.11");
        client.on("test/+/info", (topic, payload) -> {
            logger.info(topic + ": " + payload.toString(CharsetUtil.UTF_8));
        }, MqttQoS.EXACTLY_ONCE);
        client.on("testinfo/something/#", (topic, payload) -> {
            logger.info(topic + ": " + payload.toString(CharsetUtil.UTF_8));
        }, MqttQoS.EXACTLY_ONCE);
    }
}

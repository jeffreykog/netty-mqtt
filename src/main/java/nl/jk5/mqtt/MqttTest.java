package nl.jk5.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MqttTest {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        MqttClient client = new MqttClient();
        client.getClientConfig();
        client.connect("10.2.6.11");
        client.on("test/topic", (topic, payload) -> {
            client.publish("test/response", payload.copy(), MqttQoS.EXACTLY_ONCE);
            /*client.once("test/t2", (t2, p2) -> {
                logger.info("Received message: " + p2.toString(CharsetUtil.UTF_8));
                client.publish("test/2", Unpooled.copiedBuffer("Lel", CharsetUtil.UTF_8), MqttQoS.EXACTLY_ONCE).addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        logger.info("Message delivered");
                    }
                });
            });*/
            logger.info(payload.toString(CharsetUtil.UTF_8));
        }, MqttQoS.EXACTLY_ONCE);
    }
}

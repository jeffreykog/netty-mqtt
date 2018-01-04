package nl.jk5.mqtt;

/**
 * Created by Valerii Sosliuk on 12/30/2017.
 */
public interface MqttClientCallback {

    /**
     * This method is called when the connection to the server is lost.
     *
     * @param cause the reason behind the loss of connection.
     */
    public void connectionLost(Throwable cause);
}

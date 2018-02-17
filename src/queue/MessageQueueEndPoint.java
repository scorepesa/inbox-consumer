/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

import com.rabbitmq.client.AMQP;
import utils.Logging;
import java.io.IOException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 *
 * @author rube
 */
public class MessageQueueEndPoint {

    /* private static Connection connection = null;
    public static String endPointName;    
    protected String exchange;
    

    private final ThreadLocal<Channel> channels = new ThreadLocal<Channel>();*/
    static final AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
    private static Logging logger = new Logging();

    /**
     * Declare message end point and queue on status
     *
     *
     */
    public MessageQueueEndPoint() {

    }

    /**
     * Maintain and Return Thread specific channel objects
     *
     * @param endPointName
     * @param exchange
     * @param routingKey
     * @param connection
     * @return
     * @throws IOException
     */
    public static final Channel getChannel(String endPointName, String exchange,
            String routingKey, Connection connection)
            throws IOException {
        Channel channel = null;
        while (channel == null) {
            if (QueueConnection.checkConnection()) {
                if (QueueConnection.checkConnectionOpen()) {
                    if (channel == null || !channel.isOpen()) {
                        channel = connection.createChannel();
                        channel.addShutdownListener(shutdownListener);
                        System.err.println(" Changed queue name ");
                        if (endPointName != null) {
                            logger.info("Declaring end queue endPoint :" + endPointName + ", exchange :" + exchange + ", RoutingKey :" + routingKey + ":");
                            //channel.queueDeclare(endPointName, true, false, false, null);
                            //channel.exchangeDeclare(exchange, "direct", true);
                            channel.queueBind(endPointName, exchange, routingKey);
                        }
                        logger.info("Channels set");
                    }
                } else {
                    logger.info("Connection is not open");
                }
            } else {
                logger.info("Connection missing");
                QueueConnection.close();
                QueueConnection.getRabbitConnection();
            }
        }
        return channel;
    }

    protected static ShutdownListener shutdownListener = new ShutdownListener() {
        @Override
        public void shutdownCompleted(ShutdownSignalException sig) {
            logger.error("ShutdownSignal: reason: "
                    + sig.getReason() + " \nReference: " + sig.getReason()
                    + " Standard QueInitialize shutdown");

        }
    };

    protected static ConfirmListener confirmListener = new ConfirmListener() {
        @Override
        public void handleAck(long seqNo, boolean multiple) {
            logger.info("Reading confirm listener: " + seqNo + ", " + multiple);

        }

        @Override
        public void handleNack(long seqNo, boolean multiple) {
            logger.info("Reading confirm listener: " + seqNo + ", " + multiple);

        }
    };
}

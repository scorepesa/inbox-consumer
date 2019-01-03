/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import utils.Logging;
import utils.Props;

/**
 *
 * @author karuri
 */
public class Publisher extends MessageQueueEndPoint {

    private static Channel channel = null;

    private static Logging logger = new Logging();

    public Publisher() {
        super();
        logger.info(" Publisher started ");
        try {
            channel = getChannel(Props.getPublishExchange(),
                    Props.getPublishExchange(), Props.getPublishExchange(),
                    QueueConnection.getConnection());
            // channel.basicQos(100);
        } catch (IOException ex) {
            logger.error(Publisher.class.getName() + " " + ex.getMessage(), ex);
        }
    }

    public static synchronized void publishMessage(String qMessage) throws IOException {
        try {
            long nextSquenceNumber = channel.getNextPublishSeqNo();
            channel.txSelect();
            logger.info(" JSON is " + qMessage);
            channel.basicPublish(Props.getPublishExchange(), Props.getPublishExchange(),
                    builder.priority(50).contentType("text/plain").deliveryMode(1).build(),
                    qMessage.getBytes());
            channel.txCommit();
        } catch (IOException e) {
            logger.error(MessageQueueEndPoint.class.getName() + " " + e.getMessage(), e);
        }
    }
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queue;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import utils.Logging;
import utils.Props;

/**
 *
 * @author karuri
 */
public class QueueConnection {

    private static Connection connection;
    private static Logging logger = new Logging();

    public QueueConnection() {
        try {
            getRabbitConnection();
        } catch (IOException io) {
            logger.error("IOException getRabbitConnection creating connection " + io.getLocalizedMessage(), io);
        } catch (Exception e) {
            logger.error("Exception getRabbitConnection creating connection " + e.getLocalizedMessage(), e);
        }
    }

    public static boolean checkConnection() {
        logger.info("Checking connection");
        if (getConnection() != null) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean checkConnectionOpen() {
        if (getConnection().isOpen()) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     * @return @throws IOException
     */
    public static final void getRabbitConnection() throws IOException {
        if (!checkConnection()) {
            ConnectionFactory factory;
            try {
                factory = new com.rabbitmq.client.ConnectionFactory();
                factory.setRequestedHeartbeat(15);
                factory.setConnectionTimeout(5000);
                factory.setAutomaticRecoveryEnabled(true);
                factory.setTopologyRecoveryEnabled(true);

                System.out.println(" Rabbit Host " + Props.getRabbitHost());
                factory.setHost(Props.getRabbitHost());
                factory.setVirtualHost(Props.getRabbitVhost());
                factory.setUsername(Props.getRabbitUsername());
                factory.setPassword(Props.getRabbitPassword());
                factory.setPort(Integer.parseInt(Props.getRabbitPort()));
                factory.setAutomaticRecoveryEnabled(true);
                factory.setRequestedHeartbeat(15);

                // Create a new connection to MQ
                setConnection(factory.newConnection());
            } catch (IOException ex) {
                logger.error("IOException getRabbitConnection creating connection " + ex.getLocalizedMessage(), ex);
                ex.printStackTrace();
            } catch (TimeoutException ex) {
                logger.error("TimeoutException getRabbitConnection creating connection " + ex.getLocalizedMessage(), ex);
                ex.printStackTrace();
            } catch (Exception ex) {
                logger.error("Exception getRabbitConnection creating connection " + ex.getLocalizedMessage(), ex);
                ex.printStackTrace();
            }
        }
    }

    /**
     * Closes the Queue Connection. This is not needed to be called explicitly
     * as connection closure happens implicitly anyways.
     *
     * @throws IOException
     */
    public static void close() throws IOException {
        try {
            getConnection().close(); //closing connection, closes all the open channels
            setConnection(null);
        } catch (AlreadyClosedException e) {
            setConnection(null);
            logger.error("Connection Already Closed ", e);
        } catch (Exception e) {
            setConnection(null);
            logger.error("Connection Failed to close ", e);
        }
    }

    /**
     * @return the connection
     */
    public static Connection getConnection() {
        return connection;
    }

    /**
     * @param aConnection the connection to set
     */
    public static void setConnection(Connection aConnection) {
        connection = aConnection;
    }

}

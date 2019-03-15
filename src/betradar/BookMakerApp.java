/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package betradar;

import db.DBTransactions;
import db.MySQL;
import queue.Publisher;
import queue.QueueConnection;
import utils.Logging;
import utils.Props;

/**
 *
 * @author reuben
 */
public class BookMakerApp {

    private static final Props props = new Props();
    private static final Logging logging = new Logging();
    private static MySQL mySQL ;
    private static final Logging logger = new Logging();
    private static final QueueConnection queueConnection = new QueueConnection();
    private static final Publisher publisher = new Publisher();
    private static final DBTransactions dbTransactions = new DBTransactions();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        try {
            mySQL = new MySQL(logger);
            MySQL.init();
            StartSDK startSDK = new StartSDK(logger);
            startSDK.startSDK();
        } catch (Exception sdk) {
            logger.error("SDK not connecting " + sdk.getMessage(), sdk);
        }
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bikolcoo;

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
public class BikoBookMakerApp {

    private static final Props props = new Props();
    private static final Logging logging = new Logging();
    private static MySQL mySQL;
    private static Logging logger = new Logging();
    private static QueueConnection queueConnection = new QueueConnection();
    private transient static Publisher publisher = new Publisher();
    private transient static DBTransactions dbTransactions = new DBTransactions();

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

    private static void insertgameIDs() {
        for (int i = 0; i < 10000; i++) {
            String value = null;
            if (String.valueOf(i).length() == 1) {
                value = "000" + String.valueOf(i);
            } else if (String.valueOf(i).length() == 2) {
                value = "00" + String.valueOf(i);
            } else if (String.valueOf(i).length() == 3) {
                value = "0" + String.valueOf(i);
            } else {
                value = String.valueOf(i);
            }
            logger.info(" Value is " + value);
            DBTransactions.update(MySQL.getConnection(), insertGameIDQuery(value));
        }
    }

    private static String insertGameIDQuery(String number) {
        return "INSERT INTO `game_ids` (number,active,created,modified) VALUES "
                + "('" + number + "', '0', now(), now())";
    }
}

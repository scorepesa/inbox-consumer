/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bikolcoo;

import com.sportradar.unifiedodds.sdk.MarketDescriptionManager;
import com.sportradar.unifiedodds.sdk.MessageInterest;
import com.sportradar.unifiedodds.sdk.OddsFeed;
import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.OddsFeedSessionBuilder;
import com.sportradar.unifiedodds.sdk.ProducerManager;
import com.sportradar.unifiedodds.sdk.SportsInfoManager;
import com.sportradar.unifiedodds.sdk.cfg.OddsFeedConfiguration;
import com.sportradar.unifiedodds.sdk.exceptions.InitException;
import db.MySQL;
import java.io.IOException;
import java.util.Calendar;
import listeners.UOFeedListenerImpl;
import utils.Logging;
import utils.Props;

/**
 *
 * @author reuben
 */
public class StartSDK {

    private final Logging logger;
    public static ProducerManager producerManager;
    public static MarketDescriptionManager marketManager;
    public static SportsInfoManager sportsInfoManager;

    public StartSDK(Logging logger) {
        this.logger = logger;
        logger.info("Starting UOFeed logger");
    }

    public void startSDK() {

        if (MySQL.TestConnection() != 0) {
            OddsFeed oddsFeed = null;
            try {
                logger.info("Starting SDK");
                Props props = new Props();

                OddsFeedConfiguration config = OddsFeed.getConfigurationBuilder()
                        .setAccessToken(props.getAccessToken())
                        .setApiHost(props.getApiHost())
                        .build();

                GlobalEventsListener globalEventsListener = new GlobalEventsListener(logger);
                oddsFeed = new OddsFeed(globalEventsListener, config);
                OddsFeedSessionBuilder builder = oddsFeed.getSessionBuilder();

                UOFeedListenerImpl uofli = new UOFeedListenerImpl(logger);

                // access the producer manager
                producerManager = oddsFeed.getProducerManager();

                // set the last received message timestamp trough the producer - if known
                // (as an example, we set the last message received timestamp as 2 days ago)
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DATE, -props.getResendLogTime());
                producerManager.setProducerRecoveryFromTimestamp(1,
                        cal.getTime().getTime());
                // with the marketManager you can access various data about the available markets
                //marketManager = oddsFeed.getMarketDescriptionManager();

                // With the sportsInfoManager helper you can access various data about the ongoing events
                sportsInfoManager = oddsFeed.getSportsInfoManager();
                OddsFeedSession session
                        = builder.setListener(uofli).setMessageInterest(
                                MessageInterest.AllMessages).build();

                // Open the feed with all the built sessions
                oddsFeed.open();

                logger.info("The sdk is running. Hit any key to exit");
            } catch (InitException e) {
                try {
                    if (oddsFeed != null) {
                        oddsFeed.close();
                    }
                } catch (IOException ex) {
                    logger.error("Exception closing feed app thrown ", ex);
                }
                logger.error("Exception thrown ", e);
            }
        }
        while (true) {

        }
    }

}

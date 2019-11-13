/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package betradar;

import com.sportradar.unifiedodds.sdk.MarketDescriptionManager;
import com.sportradar.unifiedodds.sdk.MessageInterest;
import com.sportradar.unifiedodds.sdk.OddsFeed;
import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.OddsFeedSessionBuilder;
import com.sportradar.unifiedodds.sdk.ProducerManager;
import com.sportradar.unifiedodds.sdk.SportsInfoManager;
import com.sportradar.unifiedodds.sdk.cfg.ConfigurationBuilder;
import com.sportradar.unifiedodds.sdk.cfg.OddsFeedConfiguration;
import com.sportradar.unifiedodds.sdk.exceptions.InitException;
import db.MySQL;
import java.io.IOException;
import java.util.Calendar;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
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

                 ConfigurationBuilder cfgBuilder;

            switch(props.getEnv()){
                case 2:
                    logger.info("The sdk is ENV selectionsn 2 INTEGRATION");
                     cfgBuilder = OddsFeed.getOddsFeedConfigurationBuilder()
                            .setAccessToken(props.getAccessToken())
                            .selectIntegration();
                    break;
                case 1:
                    logger.info("The sdk is ENV selections 1 PRODUCTION ");
                     cfgBuilder = OddsFeed.getOddsFeedConfigurationBuilder()
                            .setAccessToken(props.getAccessToken())
                            .selectProduction();
                    break;

                default:
                    cfgBuilder = OddsFeed.getOddsFeedConfigurationBuilder()
                            .setAccessToken(props.getAccessToken())
                            .selectIntegration();


            }

            OddsFeedConfiguration config = cfgBuilder.setDefaultLocale(Locale.ENGLISH)
                            .setMaxRecoveryExecutionTime(props.getResendLogTime(), TimeUnit.HOURS) 
                            .setMaxInactivitySeconds(30)
                            .setSdkNodeId(17)
                            .build();

            GlobalEventsListener globalEventsListener = new GlobalEventsListener(logger);
            UOFeedListenerImpl uofli = new UOFeedListenerImpl(logger);

         
            oddsFeed = new OddsFeed(globalEventsListener, config);
            OddsFeedSessionBuilder builder = oddsFeed.getSessionBuilder();

            // access the producer manager
            producerManager = oddsFeed.getProducerManager();

            // set the last received message timestamp trough the producer - if known
            // (as an example, we set the last message received timestamp as 2 days ago)
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DATE, -props.getResendLogTime());

            producerManager.setProducerRecoveryFromTimestamp(3,
                    cal.getTime().getTime());
            // with the marketManager you can access various data about the available markets
            marketManager = oddsFeed.getMarketDescriptionManager();

            // With the sportsInfoManager helper you can access various data about the ongoing events
            sportsInfoManager = oddsFeed.getSportsInfoManager();

            OddsFeedSession session
                    = builder.setListener(uofli).setMessageInterest(
                            MessageInterest.PrematchMessagesOnly).build();

            // Open the feed with all the built sessions
            oddsFeed.open();
            logger.info("The sdk is running ON LIVE FEED Session. Hit any key to exit");
            
        } catch (InitException e) {
            try {
                if (oddsFeed != null) {
                    oddsFeed.close();
                }
            } catch (IOException ex) {
                logger.error("Exception closing feed app thrown ", ex);
            }
            logger.error("Exception thrown SDK ", e);
        }
        }
        while (true) {

        }
    }

}

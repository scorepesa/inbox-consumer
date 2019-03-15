/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package betradar;

/**
 *
 * @author rube
 */
/* Copyright (C) Sportradar AG. See LICENSE for full license governing this code */
import com.sportradar.unifiedodds.sdk.SDKGlobalEventsListener;
import com.sportradar.unifiedodds.sdk.oddsentities.ProducerDown;
import com.sportradar.unifiedodds.sdk.oddsentities.ProducerUp;
import com.sportradar.utils.URN;
import db.DBTransactions;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import utils.Logging;

public class GlobalEventsListener implements SDKGlobalEventsListener {

    private static Logging logger;
    ExecutorService executors;

    public GlobalEventsListener(Logging logger) {
        this.logger = logger;
        executors = Executors.newCachedThreadPool();
    }

    @Override
    public void onConnectionDown() {
        logger.error("Connection shutdown ... ");
    }

    @Override
    public void onProducerDown(ProducerDown pd) {
        
        executors.execute(new Thread(){
            @Override
            public void run(){
                logger.error("Connection onProducerDown ... disable betting completely ");
                String disableBetting = "insert into betting_control set id = null, "
                        + " disabled=1, created = now(), bookmaker_event='PRODUCER DOWN'  ";

                DBTransactions.update(disableBetting);
                }
        });

    }

    @Override
    public void onProducerUp(ProducerUp pu) {
        
        executors.execute(new Thread(){
            @Override
            public void run(){
                logger.info("Connection onProducerUp ... Will enable betting");
                String enableBetting = "insert into betting_control set id = null, "
                        + " disabled=0, created = now(), bookmaker_event='PRODUCER UP' ";
                //Force enable markets on producer down
                while (true) {
                    String id = DBTransactions.update(enableBetting);
                    if (id != null) {
                        break;
                    }
                }
            }
        });
        
    }

    @Override
    public void onEventRecoveryCompleted(URN urn, long l) {
        executors.execute(new Thread(){
            @Override
            public void run(){
                logger.info("Connection onEventRecoveryCompleted ... Will enable betting");
                String enableBetting = "insert into betting_control set id = null, "
                        + " disabled=0, created = now(), bookmaker_event='RECOVERY COMPLETED' ";
                //Force enable markets on producer down
                while (true) {
                    String id = DBTransactions.update(enableBetting);
                    if (id != null) {
                        break;
                    }
                }
            }
        });
    }

}

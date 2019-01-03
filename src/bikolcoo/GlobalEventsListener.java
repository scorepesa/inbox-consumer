/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bikolcoo;

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
import db.MySQL;
import utils.Logging;

public class GlobalEventsListener implements SDKGlobalEventsListener {

    private static Logging logger;

    public GlobalEventsListener(Logging logger) {
        this.logger = logger;
    }

    @Override
    public void onConnectionDown() {
        logger.error("Connection shutdown ... ");
    }

    @Override
    public void onProducerDown(ProducerDown pd) {
        logger.error("Connection onProducerDown ... disable betting completely ");
        String disableBetting = "insert into betting_control set id = null, "
                + " disabled=1, created = now() ";

        DBTransactions.update(MySQL.getConnection(),
                disableBetting);

    }

    @Override
    public void onProducerUp(ProducerUp pu) {
        logger.info("Connection onProducerUp ... Will enable betting");
        String enableBetting = "insert into betting_control set id = null, "
                + " disabled=0, created = now() ";
        //Force enable markets on producer down
        while (true) {
            String id = DBTransactions.update(MySQL.getConnection(),
                    enableBetting);
            if (id != null) {
                break;
            }
        }
    }

    @Override
    public void onEventRecoveryCompleted(URN urn, long l) {
        logger.info("Connection onEventRecoveryCompleted ... ");
    }

}

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
        logger.error("Connection onProducerDown ... ");
    }

    @Override
    public void onProducerUp(ProducerUp pu) {
        logger.info("Connection onProducerUp ... ");
    }

    @Override
    public void onEventRecoveryCompleted(URN urn, long l) {
        logger.info("Connection onEventRecoveryCompleted ... ");
    }

}

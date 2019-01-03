package listeners;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import bikolcoo.SaveMatchData;
import bikolcoo.SettleMatch;
import com.sportradar.unifiedodds.sdk.OddsFeedListener;
import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.oddsentities.BetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.BetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.BetStop;
import com.sportradar.unifiedodds.sdk.oddsentities.FixtureChange;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsChange;
import com.sportradar.unifiedodds.sdk.oddsentities.RollbackBetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.RollbackBetSettlement;
import utils.Logging;
import utils.Props;

public class UOFeedListenerImpl implements OddsFeedListener {

    private ExecutorService executor;
    private Logging logger;

    @Override
    public void onOddsChange(OddsFeedSession ofs, OddsChange<SportEvent> oc) {
        logger.info("On onOddsChange with match id: {} " + oc.toString());
        SaveMatchData saveMatchData = new SaveMatchData(logger, ofs, oc);
        executor.execute(saveMatchData);

    }

    @Override
    public void onBetStop(OddsFeedSession ofs, BetStop<SportEvent> bs) {
        SaveMatchData saveMatchData = new SaveMatchData(logger, ofs, bs);
        executor.execute(saveMatchData);
    }

    @Override
    public void onBetSettlement(OddsFeedSession ofs, BetSettlement<SportEvent> bs) {
        logger.info("On onBetSettlement with match id: {} " + bs.toString());
        SettleMatch settleMatch = new SettleMatch(logger, ofs, bs);
        Thread t = new Thread(settleMatch);
        t.start();
    }

    @Override
    public void onRollbackBetSettlement(OddsFeedSession ofs, RollbackBetSettlement<SportEvent> rbs) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onBetCancel(OddsFeedSession ofs, BetCancel<SportEvent> bc) {

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onRollbackBetCancel(OddsFeedSession ofs, RollbackBetCancel<SportEvent> rbc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onFixtureChange(OddsFeedSession ofs, FixtureChange<SportEvent> fc) {
        logger.info("On onFixtureChange ");
        SaveMatchData saveMatchData = new SaveMatchData(logger, ofs, fc);
        executor.execute(saveMatchData);
    }

    @Override
    public void onUnparseableMessage(OddsFeedSession ofs, byte[] bytes, SportEvent se) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public UOFeedListenerImpl(Logging logger) {
        executor = Executors.newFixedThreadPool(Props.getNumOfThreads());
        this.logger = logger;
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package betradar;

import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.oddsentities.BetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeResult;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeSettlement;

import db.DBTransactions;
import db.MySQL;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import queue.Publisher;
import utils.Logging;

/**
 *
 * @author karuri
 */
public class SettleMatch implements Runnable {

    private final transient BetSettlement<SportEvent> betSettlement;
    private static Logging logger;

    public SettleMatch(Logging logger,
            OddsFeedSession ofs, BetSettlement<SportEvent> bs) {
        logger.info("Bet settlement event having BD ==> " + String.valueOf(bs));
        //this.sportEvent = bs.getEvent();
        SettleMatch.logger = logger;
        this.betSettlement = bs;
    }

    private int saveData() {
        int status = -1;
        logger.info("Save Match Data received values for oddsChange ");
        String parentMatchID = this.getParentMatchId();
        logger.info("Preparing to settle event ==> " + parentMatchID);
        int[] outcomes = insertOutcomes(parentMatchID);
        String goalsID = insertGoals(parentMatchID);

        return status;
    }

    private String getParentMatchId() {
        return String.valueOf(this.betSettlement.getEvent().getId().getId());
    }

    private int[] insertOutcomes(String parentMatchID) {
        int[] outcomes = null;
        PreparedStatement ps = null;
        Connection connection = null;
        ResultSet rs = null;
        JSONObject jObject = new JSONObject();
        JSONArray jArray = new JSONArray();
        ArrayList<String[]> outcomesData = new ArrayList<>();
        logger.info("insertOutcomes Received bet settlement for sport event "
                + String.valueOf(this.betSettlement.getEvent()));

        try {
            connection = MySQL.getConnection();
            ps = connection.prepareStatement(insertOutcomesQuery(),
                    Statement.RETURN_GENERATED_KEYS);
            for (MarketWithSettlement marketSettlement : this.betSettlement.getMarkets()) {
                logger.info("Looping through Market ==> " + String.valueOf(marketSettlement));
                // Then iterate through the result for each outcome (win or loss)
                for (OutcomeSettlement result : marketSettlement.getOutcomeSettlements()) {
                    logger.info("Reading result ==> " + String.valueOf(result));

                    logger.info("Found Winnig result ==> " + String.valueOf(result));
                    jObject.put("parent_match_id", parentMatchID);
                    jObject.put("live_bet", 0);

                    String oddType = String.valueOf(marketSettlement.getId());
                    String outcomeId = result.getId();
                    String outcome = result.getName().replace("'", "''");
                    String voidFactor = String.valueOf(result.getVoidFactor());

                    Collection<String> specifiers
                            = marketSettlement.getSpecifiers().values();
                    String specialbetValue = String.join(",", specifiers);
                    String won = result.getOutcomeResult() == OutcomeResult.Won ? "1" : "0";

                    ps.setInt(1, Integer.parseInt(oddType));
                    ps.setInt(2, Integer.parseInt(parentMatchID));
                    ps.setString(3, specialbetValue);
                    ps.setString(4, outcome);
                    ps.setString(5, won);
                    ps.setString(6, voidFactor);
                    ps.setString(7, outcomeId);
                    ps.addBatch();
                    logger.info("Adding result to outcomes oddtype==> " + oddType
                            + " spv => " + specialbetValue + " outcome => " + outcome
                            + " voidFactor =>" + voidFactor
                            + "won => " + won );
                    outcomesData.add(
                            new String[]{oddType, specialbetValue, outcome, voidFactor, won});

                }
            }

            int[] res = ps.executeBatch();
            if (res != null) {
                if (res.length > 0) {
                    logger.info("Outcome Res length " + res.length);
                    int batchCounter = 0;
                    rs = ps.getGeneratedKeys();
                    while (rs.next()) {
                        int last_inserted_id = rs.getInt(1);
                        logger.info(" LAST_INSERT_ID(v_match_result_id)" + rs.getInt(1));
                        String[] outcome = outcomesData.get(batchCounter);
                        jArray.put(generateJsonMsg(
                                last_inserted_id, outcome[0], outcome[1],
                                outcome[2], outcome[3], outcome[4]));
                        batchCounter++;
                    }
                    if (jArray.length() > 0) {
                        jObject.put("outcomes", jArray);
                        Publisher.publishMessage(jObject.toString());
                    }
                }
            }

        } catch (IOException | NumberFormatException | SQLException | JSONException e) {
            logger.error("Exception thrown for parent_match_id " + parentMatchID + " " + e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    logger.fatal(SettleMatch.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.fatal(SettleMatch.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    logger.fatal(SettleMatch.class.getName() + " " + ex.getMessage(), ex);
                }
            }
        }
        return outcomes;
    }

    private String insertGoals(String parentMatchID) {
        logger.info("Received bet settlement for sport event "+ new String(this.betSettlement.getRawMessage()));
        
        String strInsertGoals = "";
        for (MarketWithSettlement marketSettlement : this.betSettlement.getMarkets()) {
            // Then iterate through the result for each outcome (win or loss)
            for (OutcomeSettlement result : marketSettlement.getOutcomeSettlements()) {
                if (result.getOutcomeResult() == OutcomeResult.Won) {
                    String id = result.getId();
                    String outcome = result.getName();

                    //if id =46 (HACK)  Halftime/Fulltime coorect score
                    if (id.equals("46")) {
                        String scoreResultHT = outcome.split("\\s+")[0];
                        String scoreResultFT = outcome.split("\\s+")[1];
                        strInsertGoals = DBTransactions.update( insertGoalsQuery(scoreResultHT, scoreResultFT, parentMatchID));
                    }
                } else {
                    System.err.println("Outcome " + result.getName() + " is a loss");
                }
            }
        }
        return strInsertGoals;
    }

    private String insertGoalsQuery(String htScore, String ftScore, String parentMatchID) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + "ht_score='" + htScore + "',ft_score='" + ftScore + "' WHERE "
                + "parent_match_id = '" + parentMatchID + "'";
    }

    private String insertOutcomesQuery() {
        return "INSERT IGNORE INTO `outcome` (sub_type_id,parent_match_id,"
                + "special_bet_value, created_by,created,modified,status,winning_outcome, "
                + " is_winning_outcome, void_factor, outcome_id ) "
                + "VALUES(?,?,?,'BETRADAR',NOW(),NOW(),'0',?, ?, ?, ?)";
    }

    private JSONObject generateJsonMsg(int outcomeSaveID, String oddType,
            String specialBetValue, String outcomeValue, String voidFactor, String won) {
        JSONObject jObject2 = new JSONObject();
        try {
            jObject2.put("outcomeSaveId", outcomeSaveID);
            jObject2.put("specialBetValue", specialBetValue);
            jObject2.put("outcomeValue", outcomeValue);
            jObject2.put("odd_type", oddType);
            jObject2.put("voidFactor", voidFactor);
            jObject2.put("won", won);

            logger.info("SUBJson created " + jObject2);
        } catch (NumberFormatException ne) {
            logger.error(SettleMatch.class.getName() + " " + ne.getMessage(), ne);
        } catch (JSONException je) {
            logger.error(SettleMatch.class.getName() + " " + je.getMessage(), je);
        } catch (Exception ex) {
            logger.error(SettleMatch.class.getName() + " " + ex.getMessage(), ex);
        }

        return jObject2;
    }

    @Override
    public void run() {
        try {
            logger.info("Attempting to save data ");
            this.saveData();
        } catch (Exception e) {
            logger.error("Exception thrown ", e);
        }
    }

}

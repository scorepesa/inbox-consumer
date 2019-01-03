/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bikolcoo;

import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.entities.LongTermEvent;
import com.sportradar.unifiedodds.sdk.entities.Match;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.entities.Tournament;
import com.sportradar.unifiedodds.sdk.impl.entities.MatchImpl;
import com.sportradar.unifiedodds.sdk.oddsentities.BetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.BetStop;
import com.sportradar.unifiedodds.sdk.oddsentities.FixtureChange;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketStatus;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsChange;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeSettlement;
import com.sportradar.utils.URN;

import db.DBTransactions;
import db.MySQL;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import queue.Publisher;
import utils.Logging;

/**
 *
 * @author karuri
 */
public class SaveMatchData implements Runnable {

    private final transient SportEvent sportEvent;
    private final transient OddsChange<SportEvent> oddsChange;
    private transient BetSettlement<SportEvent> betSettlement;
    private static Logging logger;
    private boolean stopbet = false;
    private boolean changefixture = false;

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs,
            OddsChange<SportEvent> oddsChange) {
        logger.info("Bet oddchangeEvent event having OC==> " + String.valueOf(oddsChange));
        this.sportEvent = oddsChange.getEvent();
        this.oddsChange = oddsChange;
        this.logger = logger;

    }

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs, BetSettlement<SportEvent> bs, boolean settle) {
        logger.info("Bet settlement event having BD ==> " + String.valueOf(bs));
        this.logger = logger;
        this.sportEvent = bs.getEvent();
        this.betSettlement = bs;
        this.oddsChange = null;
    }

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs, BetStop<SportEvent> bs) {
        this.logger = logger;
        this.sportEvent = bs.getEvent();
        this.oddsChange = null;
        logger.info("BetStop Event void and cancel bet");
        stopbet = true;

    }

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs, FixtureChange<SportEvent> fc) {
        this.logger = logger;
        this.sportEvent = fc.getEvent();
        this.oddsChange = null;
        this.changefixture = true;
        logger.info("Fixture change Event void and cancel bet");

    }

    private void stopBet() {
        String parentMatchID = this.getParentMatchId();
        String sql = "update `match` set bet_closure=now(), modified = now(), "
                + "status=3, priority=0 where parent_match_id = "
                + " '" + parentMatchID + "'";

        Connection connection = null;
        PreparedStatement ps = null, ps2 = null;
        try {
            connection = MySQL.getConnection();
            ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            boolean done = ps.execute();

        } catch (SQLException sqle) {
            logger.error("Unables to stop bet : " + parentMatchID, sqle);
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (ps2 != null) {
                    ps2.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                logger.error("Trouble winding up check : ", e);
            }
        }
    }

    private int saveData() {
        int status = -1;
        String parentMatchID;
        logger.info("Save Match Data received values for oddsChange ");

        if (stopbet) {
            logger.info("STOPPING BET ... ");
            this.stopBet();

        } else if (this.oddsChange != null || this.changefixture) {
            logger.info("Preparing to update odd games odds change or "
                    + "fixture change FC => " + this.changefixture);
            int sportID = insertSport();
            int competitionID = insertCompetition(sportID);
            parentMatchID = insertMatch(competitionID);
            int[] eventOddIDs = insertEventOdds(parentMatchID);
        } else {
            parentMatchID = this.getParentMatchId();
            logger.info("Preparing to settle event ==> " + parentMatchID);
            int[] outcomes = insertOutcomes(parentMatchID);
            String goalsID = insertGoals(parentMatchID);
        }
        return status;
    }

    private String getParentMatchId() {
        return String.valueOf(this.sportEvent.getId().getId());
    }

    private synchronized int[] insertOutcomes(String parentMatchID) {
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
                    String won = result.isWinning() ? "1" : "0";

                    ps.setInt(1, Integer.parseInt(oddType));
                    ps.setInt(2, Integer.parseInt(parentMatchID));
                    ps.setString(3, specialbetValue);
                    ps.setString(4, outcome);
                    ps.setString(5, (result.isWinning() ? "1" : "0"));
                    ps.setString(6, voidFactor);
                    ps.addBatch();
                    logger.info("Adding result to outcomes oddtype==> " + oddType
                            + " spv => " + specialbetValue + " outcome => " + outcome
                            + " voidFactor =>" + voidFactor
                            + "won => " + (result.isWinning() ? 1 : 0));
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
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
        }
        return outcomes;
    }

    private synchronized String insertGoals(String parentMatchID) {
        logger.info("Received bet settlement for sport event "
                + this.betSettlement.getEvent());
        String strInsertGoals = "";
        for (MarketWithSettlement marketSettlement : this.betSettlement.getMarkets()) {
            // Then iterate through the result for each outcome (win or loss)
            for (OutcomeSettlement result : marketSettlement.getOutcomeSettlements()) {
                if (result.isWinning()) {
                    String id = result.getId();
                    String outcome = result.getName();

                    //if id =46 (HACK)  Halftime/Fulltime coorect score
                    if (id.equals("46")) {
                        String scoreResultHT = outcome.split("\\s+")[0];
                        String scoreResultFT = outcome.split("\\s+")[1];
                        strInsertGoals = DBTransactions.update(MySQL.getConnection(),
                                insertGoalsQuery(scoreResultHT, scoreResultFT, parentMatchID));
                    }
                } else {
                    System.err.println("Outcome " + result.getName() + " is a loss");
                }
            }
        }
        return strInsertGoals;
    }

    private synchronized int[] insertEventOdds(String parentMatchID) {
        int[] eventOdds = null;
        logger.info("Attemting to create odd ==> " + this.oddsChange);
        PreparedStatement ps = null;
        PreparedStatement ps2 = null;
        PreparedStatement ps3 = null;
        Connection connection = null;
        try {
            connection = MySQL.getConnection();
            ps = connection.prepareStatement(insertOddsQuery(), Statement.RETURN_GENERATED_KEYS);
            ps2 = connection.prepareStatement(insertEventOddsQuery(), Statement.RETURN_GENERATED_KEYS);
            ps3 = connection.prepareStatement(insertEventOddsHistoryQuery(), Statement.RETURN_GENERATED_KEYS);
            if (this.oddsChange != null) {

                List<MarketWithOdds> marketOdds = this.oddsChange.getMarkets();

                for (MarketWithOdds mktOdds : marketOdds) {
                    String marketDescription = mktOdds.getName();
                    logger.info("Received odds information for : " + marketDescription);
                    logger.info("Market status is : " + mktOdds.getStatus());

                    int subTypeId = mktOdds.getId();
                    String parentMatchId = this.getParentMatchId();
                    String maketName = mktOdds.getName();
                    ps.setString(1, maketName);
                    ps.setInt(2, subTypeId);
                    ps.setString(3, parentMatchId);
                    ps.addBatch();

                    if (mktOdds.getStatus() != MarketStatus.Deactivated) {
                        for (OutcomeOdds outcomeOdds : mktOdds.getOutcomeOdds()) {
                            String oddValue = String.valueOf(outcomeOdds.getOdds());
                            String outcome = outcomeOdds.getName();

                            String outcomeId = outcomeOdds.getId();
                            Collection<String> specifiers = mktOdds.getSpecifiers().values();
                            String specialbetValue = String.join(",", specifiers);

                            //Check Check
                            ps2.setString(1, parentMatchID);
                            ps2.setString(2, String.valueOf(subTypeId));
                            ps2.setString(3, outcome.replace("'", "''"));
                            ps2.setString(4, oddValue);
                            ps2.setString(5, specialbetValue);
                            ps2.setString(6, oddValue);

                            ps3.setString(1, parentMatchID);
                            ps3.setString(2, String.valueOf(subTypeId));
                            ps3.setString(3, outcome.replace("'", "''"));
                            ps3.setString(4, oddValue);

                            ps2.addBatch();
                            ps3.addBatch();

                        }

                    }

                }
                int[] res = ps.executeBatch();
                eventOdds = ps2.executeBatch();
                if (eventOdds != null) {
                    ps3.executeBatch();
                }
            }

        } catch (SQLException e) {
            logger.error("Error saving odd types for parent_match_id " + parentMatchID + " "
                    + e.getMessage(), e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (ps2 != null) {
                try {
                    ps2.close();
                } catch (SQLException ex) {
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (ps3 != null) {
                try {
                    ps3.close();
                } catch (SQLException ex) {
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    logger.fatal(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
                }
            }
        }

        return eventOdds;
    }

    private synchronized String insertMatch(int competitionID) {
        String parentMatchID = null;
        try {
            parentMatchID = this.getParentMatchId();
            String gameId = null;
            ArrayList<HashMap<String, String>> matchExists = DBTransactions.query(
                    MySQL.getConnection(), matchExists(parentMatchID));

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SportEvent matchImpl = StartSDK.sportsInfoManager.getCompetition(
                    this.sportEvent.getId());

            //LongTermEvent t = ((MatchImpl) matchImpl).getTournament();
            logger.info("Dupe we found instance of Match ...yeey! " + this.sportEvent);
            Match match = ((Match) matchImpl);
            String homeTeam = match.getHomeCompetitor().getName(Locale.ENGLISH);
            String awayTeam = match.getAwayCompetitor().getName(Locale.ENGLISH);
            logger.info("Extracted match details ...homeTeam ==> " + homeTeam
                    + " Away Team " + awayTeam);

            String startTime = sdf.format(this.sportEvent.getScheduledTime());

            logger.info("Extracted date details ...startDate ==> " + startTime
            );

            logger.info("Match query result => " + String.valueOf(matchExists));
            if (matchExists == null || matchExists.isEmpty()) {
                logger.info("Inserting match StartTime => " + startTime
                        + " Home Team =>" + homeTeam + " Away team =>" + awayTeam
                        + " Parent Match ID => " + parentMatchID);

                gameId = this.getGameId();
                logger.info("Inserting match gameID " + gameId + " parentMatchID " + parentMatchID);
                String strInsertMatchID = DBTransactions.update(MySQL.getConnection(),
                        insertMatchQuery(homeTeam, awayTeam,
                                startTime, gameId,
                                parentMatchID, competitionID));
                logger.info("Inserted Match ID " + strInsertMatchID);

            } else {
                //update match time
                logger.info("Match does exist run update");
                String strUpdateMatchID = DBTransactions.update(MySQL.getConnection(),
                        updateMatchTimeQuery(startTime, competitionID,
                                matchExists.get(0).get("match_id")));

            }

        } catch (Exception exe) {
            logger.error("Exception thrown ", exe.fillInStackTrace());
            exe.printStackTrace();

        }
        return parentMatchID;
    }

    private synchronized int insertCompetition(int sportID) {
        int insertCompetitonID = -1;
        int categoryID = -1;
        try {
            //Returns an URN uniquely identifying the tournament associated with the current instance
            URN urn = this.sportEvent.getId();
            String category, countryCode = null;
            long betraderCategoryId;

            SportEvent matchImpl = StartSDK.sportsInfoManager.getCompetition(urn);

            LongTermEvent t = ((MatchImpl) matchImpl).getTournament();

            //if (t instanceof Tournament) {
            logger.info("Clearly sport event turned to be Tournament .. hhihi");
            category = ((Tournament) t).getCategory().getName(Locale.ENGLISH);
            countryCode = ((Tournament) t).getCategory().getCountryCode();
            betraderCategoryId = ((Tournament) t).getCategory().getId().getId();
            categoryID = Integer.valueOf(DBTransactions.update(MySQL.getConnection(),
                    insertCategoryQuery(category, sportID, betraderCategoryId, countryCode))
            );

            logger.info("Category ID is " + categoryID);

            if (categoryID != -1) {
                //update competition or insert
                String tornament = ((Tournament) t).getName(Locale.ENGLISH);
                ArrayList<HashMap<String, String>> competitionExists = DBTransactions.
                        query(MySQL.getConnection(), competitionExistsQuery(
                                tornament, sportID, category));
                String strInsertCompetitionID = null;
                if (!competitionExists.isEmpty()) {
                    logger.info("Did find any existing ==> "
                            + competitionExists.get(0).get("competition_id"));
                    int competitionId = Integer.parseInt(competitionExists.get(0).
                            get("competition_id"));
                    int categoryId = Integer.parseInt(competitionExists.get(0).
                            get("category_id"));

                    if (categoryId == 0) {
                        strInsertCompetitionID = DBTransactions.update(
                                MySQL.getConnection(), updateCompetitonIDQuery(categoryID,
                                competitionId));
                        insertCompetitonID = Integer.parseInt(strInsertCompetitionID);
                    } else {
                        insertCompetitonID = competitionId;
                    }
                } else {
                    logger.info("Did not find any existing competiton will create a new");
                    // do insert
                    strInsertCompetitionID = DBTransactions.update(MySQL.getConnection(),
                            insertCompetitionQuery(tornament,
                                    category, categoryID, sportID));
                    insertCompetitonID = Integer.parseInt(strInsertCompetitionID);
                    logger.info("Competiton ID is " + insertCompetitonID);
                }
            }
            //}
        } catch (NumberFormatException e) {
            logger.error("Exception thrown ", e);
        }
        return insertCompetitonID;
    }

    private synchronized int insertSport() {
        int insertSportID = -1;
        long betradarSportID;
        String sportName;
        try {
            SportEvent matchImpl = StartSDK.sportsInfoManager.getCompetition(
                    this.sportEvent.getId());

            logger.info("Sport EVENT ID in insertSport ID ==> " + this.sportEvent.getId());

            //if (matchImpl instanceof MatchImpl) {
            LongTermEvent t = ((MatchImpl) matchImpl).getTournament();

            betradarSportID = ((Tournament) t).getSportId().getId();
            sportName = ((Tournament) t).getSport().getName(Locale.ENGLISH);
            logger.info("Extracted Data : betradarSportID =" + betradarSportID
                    + " sportName = " + sportName);

            ArrayList<HashMap<String, String>> sportExists = DBTransactions.query(
                    MySQL.getConnection(), sportExistsQuery(sportName));
            String strInsertSportID = null;
            if (!sportExists.isEmpty()) {
                int sportId = Integer.parseInt(sportExists.get(0).get("sport_id"));
                int betradarSportId = Integer.parseInt(sportExists.get(0).get("betradar_sport_id"));
                //update betradar sport id
                if (betradarSportId == 0) {
                    strInsertSportID = DBTransactions.update(MySQL.getConnection(),
                            updateSportIDQuery("" + betradarSportID, sportId));
                    insertSportID = Integer.parseInt(strInsertSportID);
                } else {
                    insertSportID = sportId;
                }
            } else {
                //insert sport
                strInsertSportID = DBTransactions.update(MySQL.getConnection(),
                        insertSportQuery(sportName, "" + betradarSportID));
                insertSportID = Integer.parseInt(strInsertSportID);
                logger.info("sportID is " + insertSportID);
            }
            //} else {
            //    logger.error("Missing tournament on create sport will pass ...." + matchImpl);
            //}
        } catch (NumberFormatException e) {
            logger.error("Exception thrown ", e);
        } catch (Exception exe) {
            logger.error("Exception thrown ", exe);
        }
        return insertSportID;
    }

    private synchronized String sportExistsQuery(String sportName) {
        return "SELECT sport_id, betradar_sport_id FROM sport WHERE "
                + "sport_name = '" + sportName + "'";
    }

    private synchronized String competitionExistsQuery(String competitionName,
            int sportID, String category) {
        return "SELECT competition_id, category_id FROM competition WHERE "
                + "competition_name = '" + competitionName + "' AND sport_id = " + sportID + " "
                + "AND category = '" + category + "' ";
    }

    private synchronized String updateSportIDQuery(String betRadarSportID, int sportID) {
        return "UPDATE sport SET sport_id = LAST_INSERT_ID(sport_id),"
                + "betradar_sport_id = '" + betRadarSportID + "' WHERE "
                + "sport_id = '" + sportID + "'";
    }

    private synchronized String updateCompetitonIDQuery(int categoryID,
            int competitionID) {
        return "UPDATE competition SET competition_id = LAST_INSERT_ID(competition_id),"
                + "category_id  = " + categoryID + " WHERE "
                + "competition_id = '" + competitionID + "'";
    }

    private synchronized String insertCategoryQuery(String categoryName, int sportID,
            long betRadarCategoryId, String countryCode) {

        return "INSERT INTO `category` (category_id,category_name,status,sport_id,"
                + "created_by,created,modified,priority, betradar_category_id, country_code) VALUES ("
                + "LAST_INSERT_ID(category_id),'" + categoryName + "',1," + sportID + ","
                + "'BETRADAR',now(),now(),0, '" + betRadarCategoryId + "', '"
                + countryCode + "') ON DUPLICATE KEY UPDATE category_id = "
                + "LAST_INSERT_ID(category_id)";
    }

    private synchronized String insertCompetitionQuery(String competitionName,
            String categoryName, int categoryID, int sportID) {
        return "INSERT INTO competition (competition_id,competition_name,category,"
                + "status,category_id,sport_id,created_by,created,modified) VALUES "
                + "(LAST_INSERT_ID(competition_id),'" + competitionName + "','" + categoryName + "',"
                + "1," + categoryID + "," + sportID + ",'BETRADAR',now(),now()) ON DUPLICATE KEY "
                + "UPDATE competition_id=LAST_INSERT_ID(competition_id)";
    }

    private synchronized String insertSportQuery(String sportName, String betRadarSportID) {
        /*return "INSERT INTO `sport` (sport_id,sport_name,created_by,betradar_sport_id,"
         + "created,modified) VALUES (LAST_INSERT_ID(sport_id),'"+sportName+"',"
         + "'BETRADAR',"+betRadarSportID+", now(), now())";*/
        return "INSERT IGNORE INTO `sport` (sport_name,created_by,betradar_sport_id,created,"
                + "modified) values ('" + sportName + "','BETRADAR','" + betRadarSportID + "',now(),"
                + "now()) ";
    }

    private synchronized String updateMatchInstanceIDQuery(long threadID) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + "instance_id = '" + threadID + "' WHERE  instance_id = 0 AND "
                + "LENGTH(game_id) < 5 AND start_time <  DATE_SUB(NOW(), INTERVAL 1 DAY) "
                + "AND game_id IN (SELECT number FROM game_ids) ORDER BY match_id "
                + "LIMIT 1";
    }

    private synchronized String gameIDExistsQuery(long threadID) {
        return "SELECT m.match_id, m.game_id FROM `match` m  WHERE "
                + "instance_id = " + threadID + " ORDER BY match_id LIMIT 1";
    }

    private synchronized String updateMatchGameIDQuery(String matchID) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + "game_id = CONCAT(created,'-',game_id), instance_id = 0 WHERE "
                + "match_id = " + matchID;
    }

    private synchronized String getGameIDQuery() {
        return "SELECT number FROM game_ids WHERE LENGTH(number) > 2 AND number "
                + "NOT IN (SELECT game_id FROM `match`) LIMIT 1";
    }

    private synchronized String insertMatchQuery(String homeTeam, String awayTeam,
            String startTime, String gameID, String parentMatchID, int competitionID) {
        return "INSERT INTO `match` (home_team,away_team,start_time,game_id,"
                + "parent_match_id,competition_id,status,bet_closure,created_by,"
                + "created,modified,completed) VALUES ('" + homeTeam + "','" + awayTeam + "',"
                + "'" + startTime + "','" + gameID + "','" + parentMatchID + "','" + competitionID + "',"
                + "1,'" + startTime + "','BETRADAR',NOW(),NOW(),0)";
    }

    private synchronized String matchExists(String parentMatchID) {
        return "SELECT match_id FROM `match` WHERE parent_match_id = '" + parentMatchID + "'";
    }

    private synchronized String updateMatchTimeQuery(String startTime, int competitionID,
            String matchID) {
        return "UPDATE `match` SET  match_id = LAST_INSERT_ID(match_id),"
                + "start_time = '" + startTime + "',bet_closure = '" + startTime + "',"
                + "competition_id = " + competitionID + ", status=1, modified=NOW() WHERE "
                + "match_id = '" + matchID + "'";
    }

    private synchronized String insertOddsQuery() {
        return "INSERT IGNORE INTO odd_type (`bet_type_id`,`name`,`sub_type_id`,`created`,"
                + "`created_by`, `parent_match_id`) VALUES(LAST_INSERT_ID(bet_type_id),?,?,now(),'BETRADAR', ?)";
    }

    private synchronized String insertEventOddsQuery() {
        return "INSERT IGNORE INTO event_odd (event_odd_id,parent_match_id,sub_type_id,"
                + "odd_key,odd_value,max_bet,created, special_bet_value) VALUES "
                + "(LAST_INSERT_ID(event_odd_id),?,?,?,?,'20000',NOW(), ?) ON DUPLICATE "
                + "KEY UPDATE odd_value= ?";
    }

    private synchronized String insertEventOddsHistoryQuery() {
        return "INSERT INTO odds_history (parent_match_id,sub_type_id,odd_key,odd_value,"
                + "created, modified) VALUES (?, ?, ?,?,NOW(),NOW())";
    }

    private synchronized String insertGoalsQuery(String htScore, String ftScore, String parentMatchID) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + "ht_score='" + htScore + "',ft_score='" + ftScore + "' WHERE "
                + "parent_match_id = '" + parentMatchID + "'";
    }

    private synchronized String insertOutcomesQuery() {
        return "INSERT IGNORE INTO `outcome` (sub_type_id,parent_match_id,"
                + "special_bet_value, created_by,created,modified,status,winning_outcome, "
                + " is_winning_outcome, void_factor ) "
                + "VALUES(?,?,?,'BETRADAR',NOW(),NOW(),'0',?, ?, ?)";
    }

    private synchronized String getGameId() {
        long localthreadId = Thread.currentThread().getId();
        String gameID = null;
        String updInstanceID = DBTransactions.update(MySQL.getConnection(),
                updateMatchInstanceIDQuery(localthreadId));
        logger.info("Trying to update match GO ID " + updInstanceID);
        if (updInstanceID != null && !updInstanceID.equals("null")) {
            logger.info("Trying to update match GO ID NO NULL" + updInstanceID);
            ArrayList<HashMap<String, String>> gameIDExists = DBTransactions.query(
                    MySQL.getConnection(), gameIDExistsQuery(localthreadId));
            logger.info("Trying to update match GAME EXISTS" + String.valueOf(gameIDExists));
            if (!gameIDExists.isEmpty()) {
                String matchId = gameIDExists.get(0).get("match_id");
                gameID = gameIDExists.get(0).get("game_id");
                logger.info("we got an old gameID {} from matchId {} " + gameID + " " + matchId);

                String updGameID = null;
                while (true) {
                    updGameID = DBTransactions.update(MySQL.getConnection(),
                            updateMatchGameIDQuery(matchId));
                    logger.info("Updating game_id " + updGameID);
                    if (updGameID != null) {
                        break;
                    }
                }
            }

        } else {
            logger.info("Currently we don't have gameIds to recycle, Let's acquire a new one");
            ArrayList<HashMap<String, String>> getGameID = DBTransactions.query(
                    MySQL.getConnection(), getGameIDQuery());
            if (getGameID.isEmpty()) {
                String query = "select number from game_ids order by 1 desc limit 1";
                ArrayList<HashMap<String, String>> result = DBTransactions.query(
                        MySQL.getConnection(), query);
                int prevID = 25;
                if (!result.isEmpty()) {
                    prevID = Integer.valueOf(result.get(0).get("number"));
                }
                String createQ = "insert into game_ids ( id, number, active, "
                        + " created, modified) values(null, " + (prevID + 1)
                        + " , 1, now(), now())";
                gameID = DBTransactions.update(
                        MySQL.getConnection(), createQ);
            } else {
                gameID = getGameID.get(0).get("number");
            }
        }
        logger.info("Game ID " + gameID);
        return gameID == null ? getGameId() : gameID;
    }

    private synchronized JSONObject generateJsonMsg(int outcomeSaveID, String oddType,
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
            logger.error(SaveMatchData.class.getName() + " " + ne.getMessage(), ne);
        } catch (JSONException je) {
            logger.error(SaveMatchData.class.getName() + " " + je.getMessage(), je);
        } catch (Exception ex) {
            logger.error(SaveMatchData.class.getName() + " " + ex.getMessage(), ex);
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

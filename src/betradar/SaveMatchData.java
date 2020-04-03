/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package betradar;

import com.sportradar.unifiedodds.sdk.OddsFeedSession;
import com.sportradar.unifiedodds.sdk.entities.BasicTournament;
import com.sportradar.unifiedodds.sdk.entities.BookingStatus;
import com.sportradar.unifiedodds.sdk.entities.EventStatus;
import com.sportradar.unifiedodds.sdk.entities.LongTermEvent;
import com.sportradar.unifiedodds.sdk.entities.Match;
import com.sportradar.unifiedodds.sdk.entities.SportEvent;
import com.sportradar.unifiedodds.sdk.entities.Tournament;
import com.sportradar.unifiedodds.sdk.entities.status.MatchStatus;
import com.sportradar.unifiedodds.sdk.impl.entities.BasicTournamentImpl;
import com.sportradar.unifiedodds.sdk.impl.entities.MatchImpl;
import com.sportradar.unifiedodds.sdk.oddsentities.BetCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.BetSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.BetStop;
import com.sportradar.unifiedodds.sdk.oddsentities.FixtureChange;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketCancel;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketStatus;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.MarketWithSettlement;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsChange;
import com.sportradar.unifiedodds.sdk.oddsentities.OddsDisplayType;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeOdds;
import com.sportradar.unifiedodds.sdk.oddsentities.OutcomeResult;
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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
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

    private final  SportEvent sportEvent;
    private final  OddsChange<SportEvent> oddsChange;
    private  final BetSettlement<SportEvent> betSettlement;
    private  final BetCancel<SportEvent> betCancel;
    private  final BetStop<SportEvent> betStop;
    private static Logging logger;
    private boolean stopbet = false;
    private boolean changefixture = false;

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs,
            OddsChange<SportEvent> oddsChange) {
        logger.info("Bet oddchangeEvent event having OC==> " + String.valueOf(oddsChange));
        this.sportEvent = oddsChange.getEvent();
        this.oddsChange = oddsChange;
        this.betCancel=null;
        SaveMatchData.logger = logger;
        this.changefixture = false;
        this.betStop = null;
        this.betSettlement = null;
 
    }
    
     public SaveMatchData(Logging logger,
            OddsFeedSession ofs,
            BetCancel<SportEvent> bc) {
        SaveMatchData.logger = logger;
        logger.info("Bet oddchangeEvent event having OC==> ");
        this.sportEvent = bc.getEvent();
        this.oddsChange = null;
        this.betCancel = bc;
        this.changefixture = false;
        this.betStop = null;
        this.betSettlement = null;

    }

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs, BetSettlement<SportEvent> bs, boolean settle) {
        SaveMatchData.logger = logger;
        logger.info("Bet settlement event having BD ==> " + String.valueOf(bs));
        
        this.sportEvent = bs.getEvent();
        this.betSettlement = bs;
        this.oddsChange = null;
        this.changefixture = false;
        this.betCancel=null;
        this.betStop = null;

    }

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs, BetStop<SportEvent> bs) {
        SaveMatchData.logger = logger;
        this.sportEvent = bs.getEvent();
        this.betStop = bs;
        this.oddsChange = null;
        this.betCancel=null;
        this.changefixture = false;
        this.betSettlement = null;
        logger.info("BetStop Event void and cancel bet");
        stopbet = true;

    }

    public SaveMatchData(Logging logger,
            OddsFeedSession ofs, FixtureChange<SportEvent> fc) {
        SaveMatchData.logger = logger;
        this.sportEvent = fc.getEvent();
        this.oddsChange = null;
        this.changefixture = true;
        this.betCancel=null;
        this.betStop = null;
        this.betSettlement=null;
        logger.info("Fixture change Event void and cancel bet");

    }

    private void stopBet() {
        String parentMatchID = this.getParentMatchId();
        
        if(null != this.betStop){
             
             MarketStatus marketStatus = this.betStop.getMarketStatus();
             
             String groupNames="";            
             List<String> groupNamesList = this.betStop.getGroups();
             groupNames = groupNamesList.stream().map((g) -> "'"+g+"', ").reduce(groupNames, String::concat);
             groupNames = groupNames.substring(0, groupNames.length()-2);
             
             Match match = ((Match) sportEvent);
        
            String eventStatus = match.getEventStatus().toString();
            int eventActive = (match.getEventStatus() == EventStatus.Live)?1:0;
             
             this.processBetStop(parentMatchID, groupNames, marketStatus.toString(), 
                     eventStatus, eventActive);
        }
    }
    
    private void processBetStop(String parentMatchId, String groupNames, 
            String marketStatus, String eventStatus, int eventActive){   
  
        
        logger.info(" BET STOP | PMID "+parentMatchId+" | "
            + " Groups "+groupNames + " | MKT status " + marketStatus 
            + " | EventStatus "+ eventStatus + " | EventActive " +eventActive );
      
      
        ArrayList<String> queries = new ArrayList<>();
       
        if(groupNames.equals("all")||groupNames.contains("all")){ 
            
            String matchUpdate = "update `match` m  set m.bet_closure=now(), m.modified = now(), "
                 + " m.status=3, m.priority=0 where m.parent_match_id =  '" + parentMatchId + "'";

            queries.add(matchUpdate);
            
            String oddsChnageUpdate = "update event_odd set active = 0  where parent_match_id = '"+parentMatchId+"' and active=1 " ;
            
            queries.add(oddsChnageUpdate);
        }else {

            String affectedMarketSQL = " select sub_type_id from odd_type_group "
                + " where group_name  in ("+groupNames+") ";

            List<HashMap<String, String>> result = DBTransactions.query(affectedMarketSQL);

            if(!result.isEmpty()){
                result.forEach((record) -> {
                    queries.add(
                            "update event_odd lo  set lo.active=0 where  lo.parent_match_id = '"+parentMatchId+"' "
                             + " and sub_type_id = '"+record.get("sub_type_id")+ "' "
                    );
                 });
            }           
       }

       DBTransactions.updateMultiple(queries);
          
       
    }

    private int saveData() {
        int status = -1;
        String parentMatchID;
        logger.info("Save Match Data received values for oddsChange ");

        if (stopbet) {
            logger.info("STOPPING BET ... ");
            this.stopBet();
            
        }else if(null != this.betCancel){
            this.processBetCancel();

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
                    ps.addBatch();
                    logger.info("Adding result to outcomes oddtype==> " + oddType
                            + " spv => " + specialbetValue + " outcome => " + outcome
                            + " voidFactor =>" + voidFactor
                            + "won => " + won);
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

    private String insertGoals(String parentMatchID) {
        logger.info("Received bet settlement for sport event "
                + this.betSettlement.getEvent());
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
                        strInsertGoals = DBTransactions.update(insertGoalsQuery(scoreResultHT, scoreResultFT, parentMatchID));
                    }
                } else {
                    System.err.println("Outcome " + result.getName() + " is a loss");
                }
            }
        }
        return strInsertGoals;
    }

    private int[] insertEventOdds(String parentMatchID) {
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
                
                boolean handingOver = false;

                for (MarketWithOdds mktOdds : marketOdds) {
                    String marketDescription = mktOdds.getName();
                    
                    logger.info("Received odds information for : " + marketDescription);
                    logger.info("Market status is : " + mktOdds.getStatus());
                    String active =  (mktOdds.getStatus() == MarketStatus.Active) ? "1" : "0";
                    
                    if(mktOdds.getStatus() == MarketStatus.HandedOver){
                        handingOver = true;
                    }
                    
                    int subTypeId = mktOdds.getId();
                    String parentMatchId = this.getParentMatchId();
                    String maketName =marketDescription;
                    ps.setString(1, maketName);
                    ps.setInt(2, subTypeId);
                    ps.setString(3, parentMatchId);
                    ps.setString(4, "1");
                    ps.setString(5, active);
                    ps.addBatch();

                    
                    for (OutcomeOdds outcomeOdds : mktOdds.getOutcomeOdds()) {
                        String oddValue = String.valueOf(outcomeOdds.getOdds(OddsDisplayType.Decimal));
                        String outcome = outcomeOdds.getName();

                        String outcomeId = outcomeOdds.getId();
                        
                        Map<String, String> specifiersMap = mktOdds.getSpecifiers();
                        Collection<String> specifiers = specifiersMap.values();
                        Collection<String> specifierKeys = specifiersMap.keySet();
                        
                        
                        
                        String specialbetValue = String.join(",", specifiers);
                        String specialOddKey = String.join(",", specifierKeys);

                        //Check Check
                        ps2.setString(1, parentMatchID);
                        ps2.setString(2, String.valueOf(subTypeId));
                        ps2.setString(3, outcome.replace("'", "''"));
                        ps2.setString(4, oddValue);
                        ps2.setString(5, specialbetValue);
                        ps2.setString(6, outcomeId);
                        ps2.setString(7, specialOddKey);
                        
                        ps2.setString(8, oddValue);
                        ps2.setString(9, outcomeId);
                        ps2.setString(10, specialOddKey);

                        ps3.setString(1, parentMatchID);
                        ps3.setString(2, String.valueOf(subTypeId));
                        ps3.setString(3, outcome.replace("'", "''"));
                        ps3.setString(4, oddValue);
                        ps3.setString(5, specialbetValue);
                        

                        ps2.addBatch();
                        ps3.addBatch();

                    }


                }
                logger.info("Executing PS 1  Insert Odds QUERY " + parentMatchID + " " + ps.toString());
                int[] res = ps.executeBatch();
                logger.info("Executing PS 2  Insert EVENT Odds QUERY " + parentMatchID + " " + ps2.toString());
                eventOdds = ps2.executeBatch();
                
                if (eventOdds != null) {
                    logger.info("Executing PS 3  Insert Odds HISTORY " + parentMatchID +" " + ps3.toString());
                    ps3.executeBatch();
                   
                }
                MatchImpl match = (MatchImpl)this.sportEvent;
                
                if(handingOver && match.getBookingStatus() == BookingStatus.Booked){
                    logger.info("HANDING OVER | PMID ["+parentMatchID+"] ");
                    
                    String matchHandOver = "insert ignore into live_match (match_id, "
                            + " parent_match_id, home_team, away_team, start_time, game_id, "
                            + " competition_id, status, instance_id, bet_closure, created_by,"
                            + " created, priority, modified) select null, parent_match_id, "
                            + " home_team, away_team, start_time, game_id, competition_id, "
                            + " status, instance_id, bet_closure, created_by, created, priority, "
                            + " modified from `match` where parent_match_id='"+parentMatchID+"'";
                    
                    DBTransactions.update(matchHandOver);
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
            ArrayList<HashMap<String, String>> matchExists = DBTransactions.query(matchExists(parentMatchID));

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

                String strInsertMatchID = null;
                int trials = 0;
                
                while(strInsertMatchID == null){
                    gameId = this.getGameId();
                    logger.info("Inserting match gameID " + gameId + " parentMatchID " + parentMatchID);
                    strInsertMatchID = DBTransactions.update(insertMatchQuery(homeTeam, awayTeam,
                                    startTime, gameId, parentMatchID, competitionID));
                    trials++;
                    logger.info("Trying to  inserted Match ID " + parentMatchID + ", GameId " + gameId + " for the ["+ trials + "] times ");
                    
                    if(trials > 1000){
                        trials = 0;
                        logger.error("Insert match failed after 1000 trials aborting create match. IAM SAD");
                        break;
                    }
                }

            } else {
                //update match time
                logger.info("Match does exist run update");
                String strUpdateMatchID = DBTransactions.update(updateMatchTimeQuery(startTime, competitionID,
                                matchExists.get(0).get("match_id")));

            }

        } catch (Exception exe) {
            logger.error("Exception thrown saving match - MATCH MAY NOT BE CREATED ", exe);
     

        }
        return parentMatchID;
    }

    private int insertCompetition(int sportID) {
        int insertCompetitonID = -1;
        int categoryID;
        try {
            //Returns an URN uniquely identifying the tournament associated with the current instance
            URN urn = this.sportEvent.getId();
            String category, countryCode = null;
            long betraderCategoryId;

            SportEvent matchImpl = StartSDK.sportsInfoManager.getCompetition(urn);
            String tornament = ""; 

            LongTermEvent t = ((MatchImpl) matchImpl).getTournament();

            if (t instanceof Tournament) {
                logger.info("Clearly sport event turned to be Tournament .. hhihi");
                category = ((Tournament) t).getCategory().getName(Locale.ENGLISH);
                countryCode = ((Tournament) t).getCategory().getCountryCode();
                betraderCategoryId = ((Tournament) t).getCategory().getId().getId();
                 tornament = ((Tournament) t).getName(Locale.ENGLISH);
            }else if(t instanceof  BasicTournament){
                category = ((BasicTournament) t).getCategory().getName(Locale.ENGLISH);
                countryCode = ((BasicTournament) t).getCategory().getCountryCode();
                betraderCategoryId = ((BasicTournament) t).getCategory().getId().getId();
                tornament = ((BasicTournament) t).getName(Locale.ENGLISH);
            
            }else{
                logger.error("Coulf not get categoryName, code rekless abandon");
                return -1;
            }
            
            
            categoryID = Integer.valueOf(DBTransactions.update(insertCategoryQuery(category, sportID, betraderCategoryId, countryCode))
            );

            logger.info("Category ID is " + categoryID);

            if (categoryID != -1) {
                //update competition or insert
                
                ArrayList<HashMap<String, String>> competitionExists = DBTransactions.
                        query(competitionExistsQuery(tornament, sportID, category));
                String strInsertCompetitionID;
                if (!competitionExists.isEmpty()) {
                    logger.info("Did find any existing ==> "
                            + competitionExists.get(0).get("competition_id"));
                    int competitionId = Integer.parseInt(competitionExists.get(0).
                            get("competition_id"));
                    int categoryId = Integer.parseInt(competitionExists.get(0).
                            get("category_id"));

                    if (categoryId == 0) {
                        strInsertCompetitionID = DBTransactions.update(
                                 updateCompetitonIDQuery(categoryID,competitionId));
                        insertCompetitonID = Integer.parseInt(strInsertCompetitionID);
                    } else {
                        insertCompetitonID = competitionId;
                    }
                } else {
                    logger.info("Did not find any existing competiton will create a new");
                    // do insert
                    strInsertCompetitionID = DBTransactions.update(insertCompetitionQuery(tornament,
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

    private int insertSport() {
        int insertSportID = -1;
        long betradarSportID=-1;
        String sportName = "";
        try {
            SportEvent matchImpl = StartSDK.sportsInfoManager.getCompetition(
                    this.sportEvent.getId());

            logger.info("Sport EVENT ID in insertSport ID ==> " + this.sportEvent.getId());
            
            if (matchImpl instanceof MatchImpl) {
                LongTermEvent t = ((MatchImpl) matchImpl).getTournament();

                if(t instanceof Tournament){
                    betradarSportID = ((Tournament) t).getSportId().getId();

                    sportName = ((Tournament) t).getSport().getName(Locale.ENGLISH);
                    logger.info("Extracted Data : betradarSportID =" + betradarSportID
                            + " sportName = " + sportName);
                }else if(t instanceof  BasicTournament){
                    LongTermEvent t1 = ((MatchImpl) matchImpl).getTournament();
                    betradarSportID = ((BasicTournament) t1).getSportId().getId();

                    sportName = ((BasicTournament) t1).getSport().getName(Locale.ENGLISH);
                    logger.info("Extracted Data : betradarSportID =" + betradarSportID
                            + " sportName = " + sportName);
            
                }
            }
            
            if(sportName.equals("") || betradarSportID == -1){
                logger.error("Missing sportName form XML - returning BAD CODE");
                return -1;
            }
            
            ArrayList<HashMap<String, String>> sportExists = DBTransactions.query(
                     sportExistsQuery(sportName));
            String strInsertSportID ;
            if (!sportExists.isEmpty()) {
                int sportId = Integer.parseInt(sportExists.get(0).get("sport_id"));
                int betradarSportId = Integer.parseInt(sportExists.get(0).get("betradar_sport_id"));
                //update betradar sport id
                if (betradarSportId == 0) {
                    strInsertSportID = DBTransactions.update( updateSportIDQuery("" + betradarSportID, sportId));
                    insertSportID = Integer.parseInt(strInsertSportID);
                } else {
                    insertSportID = sportId;
                }
            } else {
                //insert sport
                strInsertSportID = DBTransactions.update(insertSportQuery(sportName, "" + betradarSportID));
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

    private String sportExistsQuery(String sportName) {
        return "SELECT sport_id, betradar_sport_id FROM sport WHERE "
                + "sport_name = '" + sportName + "'";
    }

    private String competitionExistsQuery(String competitionName,
            int sportID, String category) {
        return "SELECT competition_id, category_id FROM competition WHERE "
                + "competition_name = '" + competitionName + "' AND sport_id = " + sportID + " "
                + "AND category = '" + category + "' ";
    }

    private String updateSportIDQuery(String betRadarSportID, int sportID) {
        return "UPDATE sport SET sport_id = LAST_INSERT_ID(sport_id),"
                + "betradar_sport_id = '" + betRadarSportID + "' WHERE "
                + "sport_id = '" + sportID + "'";
    }

    private  String updateCompetitonIDQuery(int categoryID,
            int competitionID) {
        return "UPDATE competition SET competition_id = LAST_INSERT_ID(competition_id),"
                + "category_id  = " + categoryID + " WHERE "
                + "competition_id = '" + competitionID + "'";
    }

    private String insertCategoryQuery(String categoryName, int sportID,
            long betRadarCategoryId, String countryCode) {

        return "INSERT INTO `category` (category_id,category_name,status,sport_id,"
                + "created_by,created,modified,priority, betradar_category_id, country_code) VALUES ("
                + "LAST_INSERT_ID(category_id),'" + categoryName + "',1," + sportID + ","
                + "'BETRADAR',now(),now(),0, '" + betRadarCategoryId + "', '"
                + countryCode + "') ON DUPLICATE KEY UPDATE category_id = "
                + "LAST_INSERT_ID(category_id)";
    }

    private String insertCompetitionQuery(String competitionName,
            String categoryName, int categoryID, int sportID) {
        return "INSERT INTO competition (competition_id,competition_name,category,"
                + "status,category_id,sport_id,created_by,created,modified) VALUES "
                + "(LAST_INSERT_ID(competition_id),'" + competitionName + "','" + categoryName + "',"
                + "1," + categoryID + "," + sportID + ",'BETRADAR',now(),now()) ON DUPLICATE KEY "
                + "UPDATE competition_id=LAST_INSERT_ID(competition_id)";
    }

    private String insertSportQuery(String sportName, String betRadarSportID) {
        /*return "INSERT INTO `sport` (sport_id,sport_name,created_by,betradar_sport_id,"
         + "created,modified) VALUES (LAST_INSERT_ID(sport_id),'"+sportName+"',"
         + "'BETRADAR',"+betRadarSportID+", now(), now())";*/
        return "INSERT IGNORE INTO `sport` (sport_name,created_by,betradar_sport_id,created,"
                + "modified) values ('" + sportName + "','BETRADAR','" + betRadarSportID + "',now(),"
                + "now()) ";
    }

    private String updateMatchInstanceIDQuery(long threadID, int gameId) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + " instance_id = '" + threadID + "' WHERE  instance_id = 0 AND "
                + " game_id = '"+gameId+"' AND start_time <  DATE_SUB(NOW(), INTERVAL 1 DAY) LIMIT 1";
    }

    private  String gameIDExistsQuery(long threadID) {
        return "SELECT m.match_id, m.game_id FROM `match` m  WHERE "
                + "instance_id = " + threadID + " ORDER BY match_id LIMIT 1";
    }

    private String updateMatchGameIDQuery(String matchID) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + "game_id = CONCAT(created,'-',game_id), instance_id = 0 WHERE "
                + "match_id = " + matchID;
    }

    private String getGameIDQuery() {
        return "SELECT number FROM game_ids WHERE LENGTH(number) > 2 AND number "
                + "NOT IN (SELECT game_id FROM `match`) LIMIT 1";
    }

    private String insertMatchQuery(String homeTeam, String awayTeam,
            String startTime, String gameID, String parentMatchID, int competitionID) {
        return "INSERT INTO `match` (home_team,away_team,start_time,game_id,"
                + "parent_match_id,competition_id,status,bet_closure,created_by,"
                + "created,modified,completed) VALUES ('" + homeTeam + "','" + awayTeam + "',"
                + "'" + startTime + "','" + gameID + "','" + parentMatchID + "','" + competitionID + "',"
                + "1,'" + startTime + "','BETRADAR',NOW(),NOW(),0)";
    }

    private String matchExists(String parentMatchID) {
        return "SELECT match_id FROM `match` WHERE parent_match_id = '" + parentMatchID + "'";
    }

    private String updateMatchTimeQuery(String startTime, int competitionID,
            String matchID) {
        return "UPDATE `match` SET  match_id = LAST_INSERT_ID(match_id),"
                + "start_time = '" + startTime + "',bet_closure = '" + startTime + "',"
                + "competition_id = " + competitionID + ", status=1, modified=NOW() WHERE "
                + "match_id = '" + matchID + "'";
    }

    private String insertOddsQuery() {
        return "INSERT IGNORE INTO odd_type (`bet_type_id`,`name`,`sub_type_id`,`created`,"
                + " `created_by`, `parent_match_id`, live_bet, active) "
                + " VALUES(LAST_INSERT_ID(bet_type_id),?,?,now(),'BETRADAR', ?, ?, ?)";
    }

    private String insertEventOddsQuery() {
        
        return "INSERT INTO event_odd (event_odd_id,parent_match_id,sub_type_id,"
                + "odd_key,odd_value,created, special_bet_value, outcome_id, special_bet_key) VALUES "
                + "(LAST_INSERT_ID(event_odd_id),?,?,?,?,NOW(), ?, ?, ?) ON DUPLICATE "
                + "KEY UPDATE odd_value= ?, outcome_id=?, special_bet_key=?, modified=now()";
    }

    private String insertEventOddsHistoryQuery() {
        return "INSERT INTO odds_history (parent_match_id,sub_type_id,odd_key,odd_value,"
                + "created, modified, special_bet_value) VALUES (?, ?, ?,?,NOW(),NOW(), ?)";
    }

    private String insertGoalsQuery(String htScore, String ftScore, String parentMatchID) {
        return "UPDATE `match` SET match_id = LAST_INSERT_ID(match_id),"
                + "ht_score='" + htScore + "',ft_score='" + ftScore + "' WHERE "
                + "parent_match_id = '" + parentMatchID + "'";
    }

    private String insertOutcomesQuery() {
        return "INSERT IGNORE INTO `outcome` (sub_type_id,parent_match_id,"
                + "special_bet_value, created_by,created,modified,status,winning_outcome, "
                + " is_winning_outcome, void_factor ) "
                + "VALUES(?,?,?,'BETRADAR',NOW(),NOW(),'0',?, ?, ?)";
    }

    private  String getGameId() {
        long localthreadId = Thread.currentThread().getId();
        
        Random randomGenerator = new Random();
	int gameID = randomGenerator.nextInt(999999) + 1000;
        
        //int gameID = ThreadLocalRandom.current().nextInt(1000, 99999 + 1);
        
        String updInstanceID = DBTransactions.update(
                updateMatchInstanceIDQuery(localthreadId, gameID));
        
        logger.info("Trying to update match GO ID " + updInstanceID);
        boolean updated = (updInstanceID != null && updInstanceID.equals("1"));
        
        if (updated ) {
            
            logger.info("Trying to update match GO ID NO NULL" + updInstanceID);
            
            String updGameID = null;
            while (true) {
                updGameID = DBTransactions.update(updateMatchGameIDQuery(""+gameID));
                logger.info("Updating game_id " + updGameID);
                if (updGameID != null) {
                    break;
                }
            }
            return ""+gameID;
     
        } else {
            logger.info("Currently we don't have gameIds to recycle, Let's acquire a new one");
           ArrayList<HashMap<String, String>> gameIDExists = DBTransactions.query(
                    gameIDExistsQuery(gameID));
           
           logger.info("Game ID " + gameID);
           
           return gameIDExists.isEmpty() ? ""+gameID: getGameId() ;
            
    
        }
       
       
    }
    
    
    private void processBetCancel(){
        if(this.betCancel == null){
            logger.info("NuLL BetCancel Aborting ..." ); 
        }
        String  parentMatchId = this.getParentMatchId();
        
         this.betCancel.getMarkets().forEach((MarketCancel marketCancel) -> {
            String oddType = String.valueOf(marketCancel.getId());
            String voidReason = marketCancel.getVoidReason();
           
            
            if(voidReason == null){
                voidReason = "";
            }
            
            Collection<String> specifiers = marketCancel.getSpecifiers().values();
            
            String specialbetValue = String.join(",", specifiers);
           
            
            ArrayList<String> queries = new ArrayList<>();
            
            String cancelSQL ="UPDATE bet b INNER JOIN bet_slip s USING(bet_id) "
                    + " SET b.total_odd = b.total_odd/s.odd_value,"
                    + " b.raw_possible_win=(b.raw_possible_win/s.odd_value) , "
                    + " b.tax = ((b.raw_possible_win/s.odd_value)-bet_amount) *0.2, "
                    + " b.possible_win =  if(s.total_games =1, b.bet_amount, (b.raw_possible_win/s.odd_value - (((b.raw_possible_win/s.odd_value)-bet_amount) *0.2) )), "
                    + " s.odd_value=s.odd_value/s.odd_value,"
                    + " bet_pick=-1  WHERE s.parent_match_id = '"+parentMatchId+"' "
                    + " AND  s.bet_pick <> -1 and s.status in (1, 400) and sub_type_id = '"+oddType+"' "
                    + " and special_bet_value = '"+specialbetValue+"' ";
            
            logger.info("BET CANCEL SQL "+ cancelSQL);
            queries.add(cancelSQL);
            //DBTransactions.update(cancelSQL);
            
            String logBetCancel = "insert into logBetCancel set id=null, parent_match_id='"+parentMatchId+"', "
                    + " created_by ='BETRADAR', created=now(), modified=now(), sub_type_id='"+oddType+"', "
                    + " special_bet_value = '"+specialbetValue+"', void_reason = '"+voidReason+"'";
            
            logger.info("BET CANCEL logBetCancel SQL "+ logBetCancel);
            queries.add(logBetCancel);
            
            DBTransactions.updateMultiple(queries);
            
            
            //load result in MQ for processing
            PreparedStatement ps =null;
            Connection conn = null;
            ResultSet rs =null;
            try {
                conn = MySQL.getConnection();
                ps = conn.prepareStatement(insertOutcomesQuery(),
                        Statement.RETURN_GENERATED_KEYS);
                
                ps.setInt(1, Integer.parseInt(oddType));
                ps.setString(2, parentMatchId);
                ps.setString(3, specialbetValue);
                ps.setString(4, "-1");
                ps.setString(5, "1");
                ps.setString(6, "0.0");
                boolean success = ps.execute();
                
                if (success){
                    rs= ps.getGeneratedKeys();
                    if(null != rs){
                        int insertID = rs.getInt(1);
                        JSONArray jArray = new JSONArray();
                        
                        jArray.put(generateJsonMsg(
                                insertID, oddType, specialbetValue,
                                "-1", "0.0", "1"));
                        JSONObject jObject = new JSONObject();
                        if (jArray.length() > 0) {
                            jObject.put("outcomes", jArray);
                            Publisher.publishMessage(jObject.toString());
                        }
                    }
                    
                }
                
                //DDD result to Q fro processing
                
            } catch (SQLException|JSONException|IOException e) {
                logger.error("Error processing bet cancel result", e);
            }finally{
                try {
                    if(rs != null)
                        rs.close();
                     
                    if(ps != null)
                        ps.close();
                     
                    if(conn != null)
                        conn.close();
                   
                    
                } catch (SQLException e) {
                    
                }
            }
        });
               
          
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

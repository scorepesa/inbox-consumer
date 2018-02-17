/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import utils.Logging;

/**
 *
 * @author karuri
 */
public class DBTransactions {

    private static final Logging logger = new Logging();

    public DBTransactions() {
    }

    public static synchronized ArrayList<HashMap<String, String>> query(
            Connection connection, String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<HashMap<String, String>> results = new ArrayList<>();
        logger.info("Running Query: " + query);
        try {
            conn = connection;
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            if (rs.next()) {
                logger.info("ResultSet not null ...");
                ResultSetMetaData metaData = rs.getMetaData();
                String[] columns = new String[metaData.getColumnCount()];
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    columns[i - 1] = metaData.getColumnLabel(i);
                }
                rs.beforeFirst();
                while (rs.next()) {
                    HashMap<String, String> record = new HashMap<String, String>();
                    for (String col : columns) {
                        record.put(col, rs.getString(col));
                    }
                    results.add(record);
                }
            }
            logger.info("Found Query Results returning :" + results.size());
            return results;
        } catch (SQLException ex) {
            logger.error(DBTransactions.class.getName() + " Query Failed " + query, ex);
            return results;
        } catch (Exception ex) {
            logger.error(DBTransactions.class.getName() + " Query Failed " + query, ex);
            return results;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.fatal(DBTransactions.class.getName() + " " + ex.getMessage());
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    logger.fatal(DBTransactions.class.getName() + " " + ex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    logger.fatal(DBTransactions.class.getName() + " " + ex.getMessage());
                }
            }
        }
    }

    public static synchronized String update(Connection connection, String query) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String autoIncKey = null;
        logger.info("Update Query: " + query);

        try {
            conn = connection;
            stmt = conn.createStatement();
            stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();

            if (rs.next()) {
                autoIncKey = rs.getString(1);
            }
            return autoIncKey;
        } catch (SQLException ex) {
            logger.error(DBTransactions.class.getName() + " Query Failed " + query, ex);
            return null;
        } catch (Exception ex) {
            logger.error(DBTransactions.class.getName() + " Query Failed " + query, ex);
            return null;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.fatal(DBTransactions.class.getName() + " " + ex.getMessage());
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    logger.fatal(DBTransactions.class.getName() + " " + ex.getMessage());
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    logger.fatal(DBTransactions.class.getName() + " " + ex.getMessage());
                }
            }
        }
    }
}

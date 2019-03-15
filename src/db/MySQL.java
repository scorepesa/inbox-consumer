/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import utils.Logging;
import utils.Props;

/**
 *
 * @author karuri
 */
public class MySQL {

    private static DataSource datasource;
    private static Logging logger;

    public MySQL(Logging logger) {
        this.logger = logger;
    }

    public static int init() {
        int status = -1;
        try {
            PoolProperties p = new PoolProperties();
            String connection = "jdbc:mysql://" + Props.getDbHost() + ":" + Props.getDbPort()
                    + "/" + Props.getDbName() + "?allowMultiQueries=true";
            
            logger.info("jdbc:mysql://" + Props.getDbHost() + ":" + Props.getDbPort() + "/"
                    + Props.getDbName());
            p.setUrl(connection);
            p.setDefaultAutoCommit(true);
            p.setDriverClassName("com.mysql.jdbc.Driver");
            p.setUsername(Props.getDbUserName());
            p.setPassword(Props.getDbPassword());
            p.setJmxEnabled(true);
            p.setTestWhileIdle(false);
            p.setTestOnBorrow(true);
            p.setValidationQuery("SELECT 1 FROM DUAL");
            p.setTestOnReturn(false);
            p.setValidationInterval(60000);
            p.setTimeBetweenEvictionRunsMillis(30000);
            p.setMaxActive(Props.getMaxConnections());
            p.setInitialSize(100);
            p.setMaxWait(50000);
            p.setMinEvictableIdleTimeMillis(5000);
            p.setMinIdle(Props.getMaxConnections() / 2);
            p.setMaxIdle(Props.getMaxConnections());
            p.setLogAbandoned(false);
            p.setRemoveAbandoned(false);

            p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
                    + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer;"
                    + "org.apache.tomcat.jdbc.pool.interceptor.ResetAbandonedTimer");

            datasource = new DataSource(p);
            datasource.setPoolProperties(p);
            datasource.createPool();
            status = 1;
        } catch (Exception ex) {
            status = -1;
            logger.error(MySQL.class.getName() + " error creating pool " + ex.getMessage(), ex);
        }

        return status;
    }

    /**
     * <p>
     * Gets a MySQL database connection. Will throw an SQLException if there is
     * an error. The connection uses UTF-8 character encoding.
     * </p>
     *
     * <p>
     * The connection is obtained using the connect string:
     * jdbc:apache:commons:dbcp:poolName
     * </p>
     *
     * @return a MySQL connection object, null on error
     */
    public static synchronized Connection getConnection() {
        Connection conn = null;
        try {
            conn = datasource.getConnection();
            logger.info("connection availability " + conn);
        } catch (SQLException e) {
            logger.error("Error attempting to get connection", e);
        }
        return conn;
    }

    public static int TestConnection() {
        int isTestConnection = 0;
        try {
            isTestConnection = datasource.getIdle();
            logger.info("Connection info is " + isTestConnection);
        } catch (Exception ex) {
            logger.error(MySQL.class.getName() + " connection not found " + ex.getMessage(), ex);
            isTestConnection = 0;
        }
        return isTestConnection;
    }

    public static void releaseConnection() throws SQLException {
        try {
            datasource.close();
            System.err.println("connection closed");
            logger.info("Connection closed");
        } catch (Exception e) {
            logger.error("release connection failed ", e);
            System.err.println("release connection failed " + e);
        }
    }

}

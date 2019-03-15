package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Loads system properties from a file.
 *
 * @author karuri
 */
@SuppressWarnings({"FinalClass", "ClassWithoutLogger"})
public final class Props {

    /**
     * The properties file.
     */
    private static final String PROPS_FILE = "conf/bikosports.conf";
    /**
     * A list of any errors that occurred while loading the properties.
     */
    private List<String> loadErrors;
    /**
     * Info log level. Default = INFO.
     */
    private static String infoLogLevel = "INFO";
    /**
     * Error log level. Default = ERROR.
     */
    private static String errorLogLevel = "ERROR";
    /**
     * Fatal log level. Default = FATAL.
     */
    private static String fatalLogLevel = "FATAL";

    /**
     * Error log file name.
     */
    private static String infoLogFile;

    /**
     * Error log file name.
     */
    private static String errorLogFile;
    /**
     * Fatal log file name.
     */
    private static String fatalLogFile;

    private static String checkFileStorageDir = "/apps/java/betradar/";

    /**
     * Database connection pool name.
     */
    private static String dbPoolName;
    /**
     * Database user name.
     */
    private static String dbUserName;
    /**
     * Database password.
     */
    private static String dbPassword;
    /**
     * Database host.
     */
    private static String dbHost;
    /**
     * Database port.
     */
    private static String dbPort;
    /**
     * Database name.
     */
    private static String dbName;

    /**
     * Maximum database connections.
     */
    private static int maxConnections;

    /**
     * Rabbit Host
     */
    private static String rabbitHost;

    /**
     * Rabbit Username
     */
    private static String rabbitUsername;

    /**
     * Rabbit Password
     */
    private static String rabbitPassword;

    /**
     * Rabbit vHost
     */
    private static String rabbitVhost;

    /**
     * Rabbit Port
     */
    private static String rabbitPort;

    /**
     * Publish Exchange
     */
    private static String publishExchange;

    private transient static int numOfThreads;

    private String accessToken;
    private String apiHost;
    private int bookMakerId;
    private int resendLogTime;
    private int env;

    /**
     * Constructor.
     */
    public Props() {
        loadErrors = new ArrayList<String>(0);
        loadProperties(PROPS_FILE);
    }

    /**
     * Load system properties.
     *
     * @param propsFile the system properties xml file
     */
    @SuppressWarnings({"UseOfSystemOutOrSystemErr", "unchecked"})
    private void loadProperties(final String propsFile) {
        FileInputStream propsStream = null;
        Properties props;

        try {
            props = new Properties();
            File base = new File(
                    //System.getProperty("user.dir")
                    Props.class.getProtectionDomain().getCodeSource().getLocation().toURI()
            ).getParentFile();

            propsStream = new FileInputStream(new File(base, propsFile));
            props.load(propsStream);

            String error1 = "ERROR: %s is <= 0 or may not have been set";
            String error2 = "ERROR: %s may not have been set";

            // Extract the values from the configuration file                     
            setInfoLogLevel(props.getProperty("InfoLogLevel"));
            if (getInfoLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "InfoLogLevel"));
            }

            setErrorLogLevel(props.getProperty("ErrorLogLevel"));
            if (getErrorLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "ErrorLogLevel"));
            }

            setFatalLogLevel(props.getProperty("FatalLogLevel"));
            if (getFatalLogLevel().isEmpty()) {
                loadErrors.add(String.format(error2, "FatalLogLevel"));
            }

            setInfoLogFile(props.getProperty("InfoLogFile"));
            if (getInfoLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "InfoLogFile"));
            }

            setErrorLogFile(props.getProperty("ErrorLogFile"));
            if (getErrorLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "ErrorLogFile"));
            }

            setFatalLogFile(props.getProperty("FatalLogFile"));
            if (getFatalLogFile().isEmpty()) {
                loadErrors.add(String.format(error2, "FatalLogFile"));
            }

            setCheckFileStorageDir(props.getProperty("FilesDir"));
            if (getCheckFileStorageDir().isEmpty()) {
                loadErrors.add(String.format(error2, "FilesDir"));
            }

            setDbPoolName(props.getProperty("DbPoolName"));
            if (getDbPoolName().isEmpty()) {
                loadErrors.add(String.format(error2, "DbPoolName"));
            }

            setDbName(props.getProperty("DbName"));
            if (getDbName().isEmpty()) {
                loadErrors.add(String.format(error1, "DbName"));
            }

            setDbUserName(props.getProperty("DbUserName"));
            if (getDbUserName().isEmpty()) {
                loadErrors.add(String.format(error2, "DbUserName"));
            }

            setDbPassword(props.getProperty("DbPassword"));
            if (getDbPassword().isEmpty()) {
                loadErrors.add(String.format(error2, "DbPassword"));
            }

            setDbHost(props.getProperty("DbHost"));
            if (getDbHost().isEmpty()) {
                loadErrors.add(String.format(error2, "DbHost"));
            }

            setDbPort(props.getProperty("DbPort"));
            if (getDbPort().isEmpty()) {
                loadErrors.add(String.format(error2, "DbPort"));
            }

            String maxConns = props.getProperty("MaximumConnections");
            if (maxConns.isEmpty()) {
                loadErrors.add(String.format(error1,
                        "MaximumConnections"));
            } else {
                setMaxConnections(Integer.parseInt(maxConns));
                if (getMaxConnections() <= 0) {
                    loadErrors.add(String.format(error1,
                            "MaximumConnections"));
                }
            }
            
            String enVVV = props.getProperty("Env");
            if (enVVV.isEmpty()) {
                loadErrors.add(String.format(error1,
                        "Env"));
            } else {
                setEnv(Integer.parseInt(enVVV));
                if (getMaxConnections() <= 0) {
                    loadErrors.add(String.format(error1,
                            "Env"));
                }
            }

            setRabbitHost(props.getProperty("RabbitHost"));
            if (getRabbitHost().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitHost"));
            }

            setRabbitUsername(props.getProperty("RabbitUsername"));
            if (getRabbitUsername().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitUsername"));
            }

            setRabbitPassword(props.getProperty("RabbitPassword"));
            if (getRabbitPassword().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitPassword"));
            }

            setRabbitVhost(props.getProperty("RabbitVhost"));
            if (getRabbitVhost().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitVhost"));
            }

            setRabbitPort(props.getProperty("RabbitPort"));
            if (getRabbitPort().isEmpty()) {
                loadErrors.add(String.format(error2, "RabbitPort"));
            }

            setPublishExchange(props.getProperty("PublishExchange").replaceAll("\\s*,\\s*", ""));
            if (getPublishExchange().isEmpty()) {
                loadErrors.add(String.format(error2, "PublishExchange"));
            }

            setApiHost(props.getProperty("APIHOST"));
            if (getApiHost().isEmpty()) {
                loadErrors.add(String.format(error2, "APIHOST"));
            }
            setAccessToken(props.getProperty("ACCESSTOKEN"));
            if (getAccessToken().isEmpty()) {
                loadErrors.add(String.format(error2, "ACCESSTOKEN"));
            }

            String bookMId = props.getProperty("BOOKMAKERID");
            try {
                setBookMakerId(Integer.valueOf(bookMId));
            } catch (NumberFormatException e) {
                loadErrors.add(String.format(error2, "BOOKMAKERID"));
            }

            String noc = props.getProperty("NumberOfThreads");
            if (noc.isEmpty()) {
                loadErrors.add(String.format(error1, "NumberOfThreads"));
            } else {
                setNumOfThreads(Integer.parseInt(noc));
                if (getNumOfThreads() <= 0) {
                    loadErrors.add(String.format(error1,
                            "NumberOfThreads"));
                }
            }

            String rlt = props.getProperty("ResendLogTime");
            if (rlt.isEmpty()) {
                loadErrors.add(String.format(error1, "ResendLogTime"));
            } else {
                setResendLogTime(Integer.parseInt(rlt));
                if (getResendLogTime() <= 0) {
                    loadErrors.add(String.format(error1,
                            "ResendLogTime"));
                }
            }

            propsStream.close();
        } catch (NumberFormatException ne) {
            System.err.println("Exiting. String value found, Integer is "
                    + "required: " + ne.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

            System.exit(1);
        } catch (FileNotFoundException | URISyntaxException ne) {
            System.err.println("Exiting. Could not find the properties file: "
                    + ne.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

            System.exit(1);
        } catch (IOException ioe) {
            System.err.println("Exiting. Failed to load system properties: "
                    + ioe.getMessage());

            try {
                propsStream.close();
            } catch (IOException ex) {
                System.err.println("Failed to close the properties file: "
                        + ex.getMessage());
            }

            System.exit(1);
        }
    }

    /**
     * @return the infoLogLevel
     */
    public static String getInfoLogLevel() {
        return infoLogLevel;
    }

    /**
     * @param aInfoLogLevel the infoLogLevel to set
     */
    public static void setInfoLogLevel(String aInfoLogLevel) {
        infoLogLevel = aInfoLogLevel;
    }

    /**
     * @return the errorLogLevel
     */
    public static String getErrorLogLevel() {
        return errorLogLevel;
    }

    /**
     * @param aErrorLogLevel the errorLogLevel to set
     */
    public static void setErrorLogLevel(String aErrorLogLevel) {
        errorLogLevel = aErrorLogLevel;
    }

    /**
     * @return the fatalLogLevel
     */
    public static String getFatalLogLevel() {
        return fatalLogLevel;
    }

    /**
     * @param aFatalLogLevel the fatalLogLevel to set
     */
    public static void setFatalLogLevel(String aFatalLogLevel) {
        fatalLogLevel = aFatalLogLevel;
    }

    /**
     * @return the infoLogFile
     */
    public static String getInfoLogFile() {
        return infoLogFile;
    }

    /**
     * @param aInfoLogFile the infoLogFile to set
     */
    public static void setInfoLogFile(String aInfoLogFile) {
        infoLogFile = aInfoLogFile;
    }

    /**
     * @return the errorLogFile
     */
    public static String getErrorLogFile() {
        return errorLogFile;
    }

    /**
     * @param aErrorLogFile the errorLogFile to set
     */
    public static void setErrorLogFile(String aErrorLogFile) {
        errorLogFile = aErrorLogFile;
    }

    /**
     * @return the fatalLogFile
     */
    public static String getFatalLogFile() {
        return fatalLogFile;
    }

    /**
     * @param aFatalLogFile the fatalLogFile to set
     */
    public static void setFatalLogFile(String aFatalLogFile) {
        fatalLogFile = aFatalLogFile;
    }

    /**
     * @return the checkFileStorageDir
     */
    public static String getCheckFileStorageDir() {
        return checkFileStorageDir;
    }

    /**
     * @param aCheckFileStorageDir the checkFileStorageDir to set
     */
    public static void setCheckFileStorageDir(String aCheckFileStorageDir) {
        checkFileStorageDir = aCheckFileStorageDir;
    }

    /**
     * @return the dbPoolName
     */
    public static String getDbPoolName() {
        return dbPoolName;
    }

    /**
     * @param aDbPoolName the dbPoolName to set
     */
    public static void setDbPoolName(String aDbPoolName) {
        dbPoolName = aDbPoolName;
    }

    /**
     * @return the dbUserName
     */
    public static String getDbUserName() {
        return dbUserName;
    }

    /**
     * @param aDbUserName the dbUserName to set
     */
    public static void setDbUserName(String aDbUserName) {
        dbUserName = aDbUserName;
    }

    /**
     * @return the dbPassword
     */
    public static String getDbPassword() {
        return dbPassword;
    }

    /**
     * @param aDbPassword the dbPassword to set
     */
    public static void setDbPassword(String aDbPassword) {
        dbPassword = aDbPassword;
    }

    /**
     * @return the dbHost
     */
    public static String getDbHost() {
        return dbHost;
    }

    /**
     * @param aDbHost the dbHost to set
     */
    public static void setDbHost(String aDbHost) {
        dbHost = aDbHost;
    }

    /**
     * @return the dbPort
     */
    public static String getDbPort() {
        return dbPort;
    }

    /**
     * @param aDbPort the dbPort to set
     */
    public static void setDbPort(String aDbPort) {
        dbPort = aDbPort;
    }

    /**
     * @return the dbName
     */
    public static String getDbName() {
        return dbName;
    }

    /**
     * @param aDbName the dbName to set
     */
    public static void setDbName(String aDbName) {
        dbName = aDbName;
    }

    /**
     * @return the maxConnections
     */
    public static int getMaxConnections() {
        return maxConnections;
    }

    /**
     * @param aMaxConnections the maxConnections to set
     */
    public static void setMaxConnections(int aMaxConnections) {
        maxConnections = aMaxConnections;
    }

    /**
     * @return the rabbitHost
     */
    public static String getRabbitHost() {
        return rabbitHost;
    }

    /**
     * @param aRabbitHost the rabbitHost to set
     */
    public static void setRabbitHost(String aRabbitHost) {
        rabbitHost = aRabbitHost;
    }

    /**
     * @return the rabbitUsername
     */
    public static String getRabbitUsername() {
        return rabbitUsername;
    }

    /**
     * @param aRabbitUsername the rabbitUsername to set
     */
    public static void setRabbitUsername(String aRabbitUsername) {
        rabbitUsername = aRabbitUsername;
    }

    /**
     * @return the rabbitPassword
     */
    public static String getRabbitPassword() {
        return rabbitPassword;
    }

    /**
     * @param aRabbitPassword the rabbitPassword to set
     */
    public static void setRabbitPassword(String aRabbitPassword) {
        rabbitPassword = aRabbitPassword;
    }

    /**
     * @return the rabbitVhost
     */
    public static String getRabbitVhost() {
        return rabbitVhost;
    }

    /**
     * @param aRabbitVhost the rabbitVhost to set
     */
    public static void setRabbitVhost(String aRabbitVhost) {
        rabbitVhost = aRabbitVhost;
    }

    /**
     * @return the rabbitPort
     */
    public static String getRabbitPort() {
        return rabbitPort;
    }

    /**
     * @param aRabbitPort the rabbitPort to set
     */
    public static void setRabbitPort(String aRabbitPort) {
        rabbitPort = aRabbitPort;
    }

    /**
     * @return the publishExchange
     */
    public static String getPublishExchange() {
        return publishExchange;
    }

    /**
     * @param aPublishExchange the publishExchange to set
     */
    public static void setPublishExchange(String aPublishExchange) {
        publishExchange = aPublishExchange;
    }

    /**
     * @return the numOfThreads
     */
    public static int getNumOfThreads() {
        return numOfThreads;
    }

    /**
     * @param aNumOfThreads the numOfThreads to set
     */
    public static void setNumOfThreads(int aNumOfThreads) {
        numOfThreads = aNumOfThreads;
    }

    /**
     * A list of any errors that occurred while loading the properties.
     *
     * @return the loadErrors
     */
    public List<String> getLoadErrors() {
        return Collections.unmodifiableList(loadErrors);
    }

    /**
     * @return the accessToken
     */
    public String getAccessToken() {
        return accessToken;
    }

    /**
     * @param accessToken the accessToken to set
     */
    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    /**
     * @return the apiHost
     */
    public String getApiHost() {
        return apiHost;
    }

    /**
     * @param apiHost the apiHost to set
     */
    public void setApiHost(String apiHost) {
        this.apiHost = apiHost;
    }

    /**
     * @return the bookMakerId
     */
    public int getBookMakerId() {
        return bookMakerId;
    }

    /**
     * @param bookMakerId the bookMakerId to set
     */
    public void setBookMakerId(int bookMakerId) {
        this.bookMakerId = bookMakerId;
    }

    /**
     * @return the resendLogTime
     */
    public int getResendLogTime() {
        return resendLogTime;
    }

    /**
     * @param resendLogTime the resendLogTime to set
     */
    public void setResendLogTime(int resendLogTime) {
        this.resendLogTime = resendLogTime;
    }

    /**
     * @return the env
     */
    public int getEnv() {
        return env;
    }

    /**
     * @param env the env to set
     */
    public void setEnv(int env) {
        this.env = env;
    }

}

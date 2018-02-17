package utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * Initializes the log files.
 *
 * @author Reuben Paul Wafula
 */
@SuppressWarnings("FinalClass")
public final class Logging {

    /**
     * Info log.
     */
    private static Logger infoLog;
    /**
     * Error log.
     */
    private static Logger errorLog;
    /**
     * Fatal log.
     */
    private static Logger fatalLog;

    /**
     * Loaded system properties.
     */
    /**
     * Constructor.
     *
     * @param properties passed in loaded system properties
     */
    public Logging() {
        System.err.println("Testing");
        initializeLoggers();
    }

    /**
     * Initialize the log managers.
     */
    @SuppressWarnings({"CallToThreadDumpStack", "UseOfSystemOutOrSystemErr"})
    private void initializeLoggers() {
        BasicConfigurator.configure();
        infoLog = Logger.getLogger("InfoLog");
        errorLog = Logger.getLogger("ErrorLog");
        fatalLog = Logger.getLogger("FatalLog");

        PatternLayout layout = new PatternLayout();
        layout.setConversionPattern("%d{yyyy MMM dd HH:mm:ss,SSS}: %p : %m%n");

        try {
            RollingFileAppender rfaInfoLog = new RollingFileAppender(layout,
                    Props.getInfoLogFile(), true);
            rfaInfoLog.setMaxFileSize("1000MB");
            rfaInfoLog.setMaxBackupIndex(100);

            RollingFileAppender rfaErrorLog = new RollingFileAppender(layout,
                    Props.getErrorLogFile(), true);
            rfaErrorLog.setMaxFileSize("1000MB");
            rfaErrorLog.setMaxBackupIndex(100);

            RollingFileAppender rfaFatalLog = new RollingFileAppender(layout,
                    Props.getFatalLogFile(), true);
            rfaFatalLog.setMaxFileSize("1000MB");
            rfaFatalLog.setMaxBackupIndex(100);

            infoLog.addAppender(rfaInfoLog);
            errorLog.addAppender(rfaErrorLog);
            fatalLog.addAppender(rfaFatalLog);

        } catch (Exception ex) {
            System.err.println("Failed to initialize loggers... EXITING: "
                    + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }

        infoLog.setLevel(Level.toLevel(Props.getInfoLogLevel()));
        errorLog.setLevel(Level.toLevel(Props.getErrorLogLevel()));
        fatalLog.setLevel(Level.toLevel(Props.getFatalLogLevel()));

        info("Initialized Loggers...");
    }

    public String getCurrentDate() {
        SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        return dft.format(new Date());
    }

    /**
     * Log info messages.
     *
     * @param message the message content
     */
    public void info(final String message) {
        infoLog.info(getCurrentDate() + " " + Thread.currentThread().getName() + " : " + message);
    }

    /**
     * Log debug messages.
     *
     * @param message the message content
     */
    public void debug(final String message) {
        infoLog.debug(getCurrentDate() + " " + Thread.currentThread().getName() + ": " + message);
    }

    /**
     * Log error messages.
     *
     * @param message the message content
     */
    public void error(final String message) {
        errorLog.error(getCurrentDate() + " " + Thread.currentThread().getName() + ": " + message);
    }

    public void error(final String message, Throwable t) {
        errorLog.error(Thread.currentThread().getName() + ": " + message, t);
    }

    /**
     * Log fatal error messages.
     *
     * @param message the message content
     */
    public void fatal(final String message) {
        fatalLog.fatal(getCurrentDate() + " " + Thread.currentThread().getName() + ": " + message);
    }

    public void fatal(final String message, Throwable t) {
        fatalLog.fatal(getCurrentDate() + " " + Thread.currentThread().getName() + ": " + message, t);
    }
}

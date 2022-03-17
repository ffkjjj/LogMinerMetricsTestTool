import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SqlUtils;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

/**
 * @author zhul
 * @date 2022/3/16 11:04
 * @description
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final String STRATEGY = "redo_log";

    private static final boolean IS_CONTINUOUS_MINING = false;

    public static void main(String[] args) throws SQLException {
        LOGGER.info("========== Start mining ==========");
        LOGGER.info("Starting redo log mining");
        Main main = new Main();
        main.initializeRedoLogsForMining();
        LOGGER.info("========== End mining ==========\n");
    }

    private void initializeRedoLogsForMining() throws SQLException {
        String url = "jdbc:oracle:thin:@//172.16.18.16:1521/oracle";
        String user = "ogg";
        String password = "ogg";
        url = "jdbc:oracle:thin:@//192.168.62.31:1521/LHR11G";
        user = "zl";
        password = "zl123";
        long startScn = 13196034L;
        long scnBatch = 0L;
        Long endScn = 13235763L;

        Instant now = Instant.now();
        OracleConnection connection = new OracleConnection(url, user, password);
        LOGGER.info("Connecting to {} cost {}", url, Duration.between(now, Instant.now()));

        LOGGER.info("Initializing redo logs for mining");
        buildDataDictionary(connection);
        setLogFilesForMining(connection, startScn);
        startMiningSession(connection, startScn, getEndScn(startScn, endScn, scnBatch));

        connection.close();
        LOGGER.info("connection closed");
    }

    private long getEndScn(long startScn, Long endScn, long scnBatch) {
        if (endScn == null) {
            return startScn + scnBatch;
        }
        return endScn;
    }

    private void setLogFilesForMining(OracleConnection connection, long startScn) throws SQLException {
        Instant start = Instant.now();
        LogMinerHelper.setLogFilesForMining(connection, startScn, Duration.ZERO, false, null);
        LOGGER.info("set log files for mining cost {}", Duration.between(start, Instant.now()));
    }

    public void startMiningSession(OracleConnection connection, long startScn, long endScn) throws SQLException {
        LOGGER.info("Starting mining session startScn={}, endScn={}, strategy={}, continuous={}",
                startScn, endScn, STRATEGY, IS_CONTINUOUS_MINING);
        try {
            Instant start = Instant.now();
            connection.executeWithoutCommitting(SqlUtils.startLogMinerStatement(startScn, endScn, IS_CONTINUOUS_MINING));
            LOGGER.info("start mining session cost {}", Duration.between(start, Instant.now()));
        } catch (SQLException e) {
            throw e;
        }
    }

    private void buildDataDictionary(OracleConnection connection) throws SQLException {
        LOGGER.info("Building data dictionary");
        Instant start = Instant.now();
        connection.executeWithoutCommitting("BEGIN DBMS_LOGMNR_D.BUILD (options => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
        LOGGER.info("Build data dictionary cost {}", Duration.between(start, Instant.now()));
    }
}

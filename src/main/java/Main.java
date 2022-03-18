import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.OracleHelper;
import util.SqlUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

    private static final Logger LOGGER_REDO_SQL = LoggerFactory.getLogger("REDO_SQL");

    private static final String STRATEGY = "redo_log";

    private static final boolean IS_CONTINUOUS_MINING = false;

    private static String url = "jdbc:oracle:thin:@//172.16.18.16:1521/oracle";
    private static String user = "zl";
    private static String password = "zl123";
    private static long startScn = 13196034L;
    private static long endScn = 14777862L;
    private static long scnBatch = 0;

    public static void main(String[] args) throws SQLException {
        LOGGER.info("========== Start mining ==========");
        initParams(args);
        LOGGER.info("Starting redo log mining");
        Main main = new Main();
        main.initializeRedoLogsForMining();
        LOGGER.info("========== End mining ==========\n");
    }

    private static void initParams(String[] args) {
        if (args.length == 0) {
            return;
        }
        url = args[0];
        user = args[1];
        password = args[2];
        startScn = Long.parseLong(args[3]);
        endScn = Long.parseLong(args[4]);
        scnBatch = Long.parseLong(args[5]);
    }

    private void initializeRedoLogsForMining() throws SQLException {
        Instant now = Instant.now();
        OracleConnection connection = new OracleConnection(url, user, password);
        LOGGER.info("Connecting to {} cost {}", url, Duration.between(now, Instant.now()));

        LOGGER.info("Initializing redo logs for mining");
        LOGGER.info("startScn={}, endScn={}, gap={}", startScn, endScn, endScn - scnBatch);
        buildDataDictionary(connection);
        setLogFilesForMining(connection, startScn);
        startMiningSession(connection, startScn, endScn);
        // queryLogMinerContents(connection, startScn, endScn);
        endMiningSession(connection);

        printOracleMetrics(connection);
        connection.close();
        LOGGER.info("Connection closed");
    }

    private void printOracleMetrics(OracleConnection connection) throws SQLException {
        OracleHelper.printSGA(connection.connection());
        OracleHelper.printMetrics(connection.connection(), "session uga memory");
        OracleHelper.printMetrics(connection.connection(), "session uga memory max");
        OracleHelper.printMetrics(connection.connection(), "session pga memory");
        OracleHelper.printMetrics(connection.connection(), "session pga memory max");
        OracleHelper.printPGA(connection.connection());
    }

    public void endMiningSession(OracleConnection connection) throws SQLException {
        Instant now = Instant.now();
        try {
            LOGGER.info("Ending mining session");
            connection.executeWithoutCommitting("BEGIN SYS.DBMS_LOGMNR.END_LOGMNR(); END;");
        }
        catch (SQLException e) {
            if (e.getMessage().toUpperCase().contains("ORA-01307")) {
                LOGGER.info("LogMiner mining session is already closed.");
                return;
            }
            // LogMiner failed to terminate properly, a restart of the connector will be required.
            throw e;
        }
        LOGGER.info("Ending mining session cost {}", Duration.between(now, Instant.now()));
    }

    private void queryLogMinerContents(OracleConnection connection, long startScn, Long endScn) throws SQLException {
        LOGGER.info("Query log miner contents");
        LOGGER_REDO_SQL.info("========== Start query log miner contents ==========");
        Instant now = Instant.now();
        try (PreparedStatement statement = createQueryStatement(connection)) {
            statement.setFetchSize(8192);
            statement.setFetchDirection(ResultSet.FETCH_FORWARD);
            statement.setString(1, String.valueOf(startScn));
            statement.setString(2, String.valueOf(endScn));

            try (ResultSet resultSet = statement.executeQuery()) {
                int count = 0;
                while (resultSet.next()) {
                    count++;
                    LOGGER_REDO_SQL.info("{}", SqlUtils.getSqlRedo(resultSet));
                }
                LOGGER_REDO_SQL.info("{} redo log records fetched", count);
            }
        }
        LOGGER.info(
                "Query log miner contents cost {}, see redo sql log files to know more details",
                Duration.between(now, Instant.now())
        );
        LOGGER_REDO_SQL.info("========== End query log miner contents ==========\n");
    }

    private PreparedStatement createQueryStatement(OracleConnection connection) throws SQLException {
        String sql = SqlUtils.queryLogMinerContents();
        return connection.connection().prepareStatement(
                sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT
        );
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
        LOGGER.info("Set log files for mining cost {}", Duration.between(start, Instant.now()));
    }

    public void startMiningSession(OracleConnection connection, long startScn, long endScn) throws SQLException {
        LOGGER.info("Starting mining session startScn={}, endScn={}, strategy={}, continuous={}",
                startScn, endScn, STRATEGY, IS_CONTINUOUS_MINING);
        try {
            Instant start = Instant.now();
            connection.executeWithoutCommitting(SqlUtils.startLogMinerStatement(startScn, endScn, IS_CONTINUOUS_MINING));
            LOGGER.info("Start mining session cost {}", Duration.between(start, Instant.now()));
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

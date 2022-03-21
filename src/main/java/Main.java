import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.OracleHelper;
import util.SqlUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Scanner;

/**
 * @author zhul
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static final Logger LOGGER_REDO_SQL = LoggerFactory.getLogger("REDO_SQL");

    private static final String STRATEGY = "redo_log";

    private static final boolean IS_CONTINUOUS_MINING = false;

    private String url = "jdbc:oracle:thin:@//192.168.62.31:1521/LHR11G";
    private String user = "zl";
    private String password = "zl123";
    private long startScn = 13196034L;
    private long endScn = 14777862L;
    private long scnBatch = 0;
    private boolean scnSetManually = false;

    public static void main(String[] args) throws SQLException {
        LOGGER.info("========== Start mining ==========");
        LOGGER.info("Starting redo log mining");
        Main main = new Main();
        main.initParams(args);
        main.initializeRedoLogsForMining();
        LOGGER.info("========== End mining ==========\n");
    }

    private void initParams(String[] args) {
        if (args.length == 0) {
            return;
        }
        if (args.length <= 3) {
            this.url = args[0];
            this.user = args[1];
            this.password = args[2];
        }
        if (args.length <= 6 && args.length > 3) {
            this.startScn = Long.parseLong(args[3]);
            this.endScn = Long.parseLong(args[4]);
            this.scnBatch = Long.parseLong(args[5]);
            LOGGER.info(
                    "params: url={}, user={}, password={}, startScn={}, endScn={}, scnBatch={}",
                    url, user, password, startScn, endScn, scnBatch
            );
        } else {
            scnSetManually = true;
            LOGGER.info(
                    "params: url={}, user={}, password={}, scn not defined, it will be configured later",
                    url, user, password
            );
        }
    }

    private void initializeRedoLogsForMining() throws SQLException {
        Instant now = Instant.now();
        OracleConnection connection = new OracleConnection(url, user, password);
        LOGGER.info("Connecting to {} cost {}", url, Duration.between(now, Instant.now()));

        configureScnIfNeeded(connection);

        LOGGER.info("Initializing redo logs for mining");
        LOGGER.info("startScn={}, endScn={}, gap={}", startScn, endScn, endScn + scnBatch - startScn);
        buildDataDictionary(connection);
        setLogFilesForMining(connection, startScn);
        startMiningSession(connection, startScn, endScn);
        // queryLogMinerContents(connection, startScn, endScn);
        endMiningSession(connection);

        printOracleMetrics(connection);
        connection.close();
        LOGGER.info("Connection closed");
    }

    private void configureScnIfNeeded(OracleConnection connection) {
        if (!scnSetManually) {
            return;
        }
        LOGGER.info("Configuring scn");
        final String[] minAndMaxScnArray = LogMinerHelper.getMinAndMaxScn(connection);
        LOGGER.info("min scn: {}", minAndMaxScnArray[0]);
        LOGGER.info("max scn: {}", minAndMaxScnArray[2]);
        LOGGER.info("min scn of last archived log file: {}", minAndMaxScnArray[1]);

        final Scanner scanner = new Scanner(System.in);
        System.out.print("Please input start scn: ");
        final String minScn = scanner.next();
        System.out.print("Please input end scn: ");
        final String maxScn = scanner.next();
        System.out.print("Please input scn batch: ");
        final String scnBatch = scanner.next();

        this.startScn = Long.parseLong(minScn);
        this.endScn = Long.parseLong(maxScn);
        this.scnBatch = Long.parseLong(scnBatch);

        LOGGER.info("startScn={}, endScn={}, scnBatch={}", startScn, endScn, scnBatch);
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
        } catch (SQLException e) {
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

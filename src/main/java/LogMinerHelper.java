import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.SqlUtils;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author zhul
 */
public class LogMinerHelper {

    private static final String CURRENT = "CURRENT";

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMinerHelper.class);

    public static void removeLogFilesFromMining(OracleConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.connection().prepareStatement("SELECT FILENAME AS NAME FROM V$LOGMNR_LOGS");
             ResultSet result = ps.executeQuery()) {
            Set<String> files = new LinkedHashSet<>();
            while (result.next()) {
                files.add(result.getString(1));
            }
            for (String fileName : files) {
                executeCallableStatement(conn, SqlUtils.deleteLogFileStatement(fileName));
                LOGGER.info("File {} was removed from mining", fileName);
            }
        }
    }

    private static void executeCallableStatement(OracleConnection connection, String statement) throws SQLException {
        Objects.requireNonNull(statement);
        try (CallableStatement s = connection.connection().prepareCall(statement)) {
            s.execute();
        }
    }

    public static void setLogFilesForMining(
            OracleConnection connection,
            long lastProcessedScn,
            long endScn,
            Duration archiveLogRetention,
            boolean archiveLogOnlyMode,
            String archiveDestinationName
    ) throws SQLException {
        removeLogFilesFromMining(connection);

        List<LogFile> logFilesForMining = getLogFilesForOffsetScn(connection, lastProcessedScn, endScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName);
        if (logFilesForMining.stream().noneMatch(l -> l.getFirstScn() <= lastProcessedScn)) {
            Long minScn = logFilesForMining.stream()
                    .map(LogFile::getFirstScn)
                    .min(Long::compareTo)
                    .orElse(null);

            if ((minScn == null || logFilesForMining.isEmpty()) && archiveLogOnlyMode) {
                throw new RuntimeException("The log.mining.archive.log.only mode was recently enabled and the offset SCN " +
                        lastProcessedScn + "is not yet in any available archive logs. " +
                        "Please perform an Oracle log switch and restart the connector.");
            }
            throw new IllegalStateException("None of log files contains offset SCN: " + lastProcessedScn + ", re-snapshot is required.");
        }

        List<String> logFilesNames = logFilesForMining.stream().map(LogFile::getFileName).collect(Collectors.toList());
        /*
        int j = logFilesNames.size();
        int i = Math.min(j - 155, j);
        while (i-- > 0) {
            logFilesNames.remove(logFilesNames.size() - i - 1);
        }
        */
        printToMinedLogFilesSizes(connection, logFilesNames);
        for (String file : logFilesNames) {
            LOGGER.trace("Adding log file {} to mining session", file);
            String addLogFileStatement = SqlUtils.addLogFileStatement("DBMS_LOGMNR.ADDFILE", file);
            executeCallableStatement(connection, addLogFileStatement);
        }

        LOGGER.info("Last mined SCN: {}, Log file list to mine: {}", lastProcessedScn, logFilesNames);
    }

    private static void printToMinedLogFilesSizes(OracleConnection connection, List<String> logFilesForMining) {
        try {
            LOGGER.info("Log files count: {}", logFilesForMining.size());
            connection.query(SqlUtils.archiveLogBytesSizeQuery(logFilesForMining), rs -> {
                if (rs.next()) {
                    double mb = rs.getLong(1) / (double) (1024 * 1024);
                    DecimalFormat df = new DecimalFormat("#.##");
                    LOGGER.info(
                            "Total size of log files to mine: {} GB({} MB)",
                            df.format(mb / 1024),
                            df.format(mb)
                    );
                } else {
                    LOGGER.info("No log files total sizes were found");
                }
            });
        } catch (SQLException e) {
            LOGGER.error("Failed to get archive log sizes", e);
        }
    }

    public static List<LogFile> getLogFilesForOffsetScn(OracleConnection connection, long offsetScn, long endScn, Duration archiveLogRetention, boolean archiveLogOnlyMode,
                                                        String archiveDestinationName)
            throws SQLException {
        LOGGER.info("Getting logs to be mined for offset scn {}", offsetScn);

        final List<LogFile> logFiles = new ArrayList<>();
        final List<LogFile> onlineLogFiles = new ArrayList<>();
        final List<LogFile> archivedLogFiles = new ArrayList<>();

        AtomicInteger count = new AtomicInteger();
        connection.query(SqlUtils.allMinableLogsQuery(offsetScn, endScn, archiveLogRetention, archiveLogOnlyMode, archiveDestinationName), rs -> {
            LOGGER.info("Processing log files");
            while (rs.next()) {
                count.getAndIncrement();
                String fileName = rs.getString(1);
                long firstScn = Long.parseLong(rs.getString(2));
                long nextScn = getNextScn(rs.getString(3));
                String status = rs.getString(5);
                String type = rs.getString(6);
                Long sequence = rs.getLong(7);
                if ("ARCHIVED".equals(type)) {
                    // archive log record
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.ARCHIVE);
                    if (logFile.getNextScn() >= offsetScn) {
                        LOGGER.trace("Archive log {} with SCN range {} to {} sequence {} to be added.", fileName, firstScn, nextScn, sequence);
                        archivedLogFiles.add(logFile);
                    }
                } else if ("ONLINE".equals(type)) {
                    LogFile logFile = new LogFile(fileName, firstScn, nextScn, sequence, LogFile.Type.REDO, CURRENT.equalsIgnoreCase(status));
                    if (logFile.isCurrent() || logFile.getNextScn() >= offsetScn) {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be added.", fileName, firstScn, nextScn, status, sequence);
                        onlineLogFiles.add(logFile);
                    } else {
                        LOGGER.trace("Online redo log {} with SCN range {} to {} ({}) sequence {} to be excluded.", fileName, firstScn, nextScn, status, sequence);
                    }
                }
            }
        });

        LOGGER.info("Resulting log files to mine: {}", count);
        LOGGER.info("Found {} archived logs and {} online logs", archivedLogFiles.size(), onlineLogFiles.size());

        // DBZ-3563
        // To avoid duplicate log files (ORA-01289 cannot add duplicate logfile)
        // Remove the archive log which has the same sequence number.
        for (LogFile redoLog : onlineLogFiles) {
            archivedLogFiles.removeIf(f -> {
                if (f.getSequence().equals(redoLog.getSequence())) {
                    LOGGER.trace("Removing archive log {} with duplicate sequence {} to {}", f.getFileName(), f.getSequence(), redoLog.getFileName());
                    return true;
                }
                return false;
            });
        }
        logFiles.addAll(archivedLogFiles);
        logFiles.addAll(onlineLogFiles);

        return logFiles;
    }

    private static long getNextScn(String longs) {
        try {
            return Long.parseLong(longs);
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    public static String[] getMinAndMaxScn(OracleConnection connection) throws SQLException {
        String[] result = new String[4];
        try {
            connection.query(SqlUtils.getMinAndMaxScnQuery(), rs -> {
                if (rs.next()) {
                    // min scn
                    result[0] = rs.getString(1);
                    // min scn of last archived log file
                    result[1] = rs.getString(2);
                    // max scn
                    result[2] = rs.getString(3);
                }
            });
        } catch (SQLException e) {
            throw new SQLException("Failed to get min and max scn", e);
        }
        try {
            result[3] = String.valueOf(getCurrentScn(connection));
        } catch (SQLException e) {
            throw new SQLException("Failed to get current scn", e);
        }
        return result;
    }

    private static long getCurrentScn(OracleConnection connection) throws SQLException {
        String sql = "SELECT CURRENT_SCN FROM V$DATABASE";
        AtomicLong currentScn = new AtomicLong();
        connection.query(sql, rs -> {
            if (rs.next()) {
                currentScn.set(rs.getLong(1));
                return;
            }
            throw new SQLException("Could not get SCN");
        });
        return currentScn.get();
    }
}

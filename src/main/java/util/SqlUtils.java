package util;

import com.sun.tools.javac.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

/**
 * @author zhul
 * @date 2022/3/16 11:53
 * @description
 */
public class SqlUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqlUtils.class);

    private static final int SQL_REDO = 2;
    private static final int CSF = 6;

    public static final List<String> EXCLUDED_SCHEMAS = Collections.unmodifiableList(Arrays.asList("appqossys", "audsys",
            "ctxsys", "dvsys", "dbsfwuser", "dbsnmp", "gsmadmin_internal", "lbacsys", "mdsys", "ojvmsys", "olapsys",
            "orddata", "ordsys", "outln", "sys", "system", "wmsys", "xdb"));

    private static final String LOGMNR_CONTENTS_VIEW = "V$LOGMNR_CONTENTS";

    private static final String LOGMNR_FLUSH_TABLE = "LOG_MINING_FLUSH";

    private static final String DATABASE_VIEW = "V$DATABASE";
    private static final String LOG_VIEW = "V$LOG";
    private static final String LOGFILE_VIEW = "V$LOGFILE";
    private static final String ARCHIVED_LOG_VIEW = "V$ARCHIVED_LOG";
    private static final String ARCHIVE_DEST_STATUS_VIEW = "V$ARCHIVE_DEST_STATUS";
    private static final String ALL_LOG_GROUPS = "ALL_LOG_GROUPS";

    public static String allMinableLogsQuery(
            long scn,
            Duration archiveLogRetention,
            boolean archiveLogOnlyMode,
            String archiveDestinationName
    ) {
        final StringBuilder sb = new StringBuilder();
        if (!archiveLogOnlyMode) {
            sb.append("SELECT MIN(F.MEMBER) AS FILE_NAME, L.FIRST_CHANGE# FIRST_CHANGE, L.NEXT_CHANGE# NEXT_CHANGE, L.ARCHIVED, ");
            sb.append("L.STATUS, 'ONLINE' AS TYPE, L.SEQUENCE# AS SEQ, 'NO' AS DICT_START, 'NO' AS DICT_END ");
            sb.append("FROM ").append(LOGFILE_VIEW).append(" F, ").append(LOG_VIEW).append(" L ");
            sb.append("LEFT JOIN ").append(ARCHIVED_LOG_VIEW).append(" A ");
            sb.append("ON A.FIRST_CHANGE# = L.FIRST_CHANGE# AND A.NEXT_CHANGE# = L.NEXT_CHANGE# ");
            sb.append("WHERE A.FIRST_CHANGE# IS NULL ");
            sb.append("AND F.GROUP# = L.GROUP# ");
            sb.append("GROUP BY F.GROUP#, L.FIRST_CHANGE#, L.NEXT_CHANGE#, L.STATUS, L.ARCHIVED, L.SEQUENCE# ");
            sb.append("UNION ");
        }
        sb.append("SELECT A.NAME AS FILE_NAME, A.FIRST_CHANGE# FIRST_CHANGE, A.NEXT_CHANGE# NEXT_CHANGE, 'YES', ");
        sb.append("NULL, 'ARCHIVED', A.SEQUENCE# AS SEQ, A.DICTIONARY_BEGIN, A.DICTIONARY_END ");
        sb.append("FROM ").append(ARCHIVED_LOG_VIEW).append(" A ");
        sb.append("WHERE A.NAME IS NOT NULL ");
        sb.append("AND A.ARCHIVED = 'YES' ");
        sb.append("AND A.STATUS = 'A' ");
        sb.append("AND A.NEXT_CHANGE# > ").append(scn).append(" ");
        sb.append("AND A.DEST_ID IN (").append(localArchiveLogDestinationsOnlyQuery(archiveDestinationName)).append(") ");

        if (!archiveLogRetention.isNegative() && !archiveLogRetention.isZero()) {
            sb.append("AND A.FIRST_TIME >= SYSDATE - (").append(archiveLogRetention.toHours()).append("/24) ");
        }

        return sb.append("ORDER BY 7").toString();
    }

    public static String deleteLogFileStatement(String fileName) {
        return "BEGIN SYS.DBMS_LOGMNR.REMOVE_LOGFILE(LOGFILENAME => '" + fileName + "');END;";
    }

    public static String addLogFileStatement(String option, String fileName) {
        return "BEGIN sys.dbms_logmnr.add_logfile(LOGFILENAME => '" + fileName + "', OPTIONS => " + option + ");END;";
    }

    public static String startLogMinerStatement(long startScn, long endScn, boolean isContinuousMining) {
        String miningStrategy;
        if (true) {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING ";
        } else {
            miningStrategy = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG ";
        }
        if (isContinuousMining) {
            miningStrategy += " + DBMS_LOGMNR.CONTINUOUS_MINE ";
        }
        return "BEGIN sys.dbms_logmnr.start_logmnr(" +
                "startScn => '" + startScn + "', " +
                "endScn => '" + endScn + "', " +
                "OPTIONS => " + miningStrategy +
                " + DBMS_LOGMNR.NO_ROWID_IN_STMT);" +
                "END;";
    }

    private static String localArchiveLogDestinationsOnlyQuery(String archiveDestinationName) {
        final StringBuilder query = new StringBuilder(256);
        query.append("SELECT DEST_ID FROM ").append(ARCHIVE_DEST_STATUS_VIEW).append(" WHERE ");
        query.append("STATUS='VALID' AND TYPE='LOCAL' ");
        if (Strings.isNullOrEmpty(archiveDestinationName)) {
            query.append("AND ROWNUM=1");
        } else {
            query.append("AND UPPER(DEST_NAME)='").append(archiveDestinationName.toUpperCase()).append("'");
        }
        return query.toString();
    }

    public static String archiveLogBytesSizeQuery(List<String> logFileNames) {
        return "SELECT SUM(blocks*block_size) FROM " + ARCHIVED_LOG_VIEW + " WHERE NAME IN ('" +
                String.join("','", logFileNames) +
                "')";
    }

    public static String queryLogMinerContents() {
        final StringBuilder query = new StringBuilder(1024);
        query.append("SELECT SCN, SQL_REDO, OPERATION_CODE, TIMESTAMP, XID, CSF, TABLE_NAME, SEG_OWNER, OPERATION, ");
        query.append("USERNAME, ROW_ID, ROLLBACK, RS_ID ");
        query.append("FROM ").append(LOGMNR_CONTENTS_VIEW).append(" ");

        // These bind parameters will be bound when the query is executed by the caller.
        query.append("WHERE SCN > ? AND SCN <= ? ");

        // Restrict to configured PDB if one is supplied
        final String pdbName = "";
        if (!Strings.isNullOrEmpty(pdbName)) {
            query.append("AND ").append("SRC_CON_NAME = '").append(pdbName.toUpperCase()).append("' ");
        }

        query.append("AND (");

        // Always include START, COMMIT, MISSING_SCN, and ROLLBACK operations
        query.append("(OPERATION_CODE IN (6,7,34,36)");

        if (true) {
            // In this mode, the connector will always be fed DDL operations for all tables even if they
            // are not part of the inclusion/exclusion lists.
            query.append(" OR ").append(buildDdlPredicate()).append(" ");
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (false) {
                query.append(") OR (OPERATION_CODE IN (1,2,3,9,10,11,29) ");
            }
            else {
                query.append(") OR (OPERATION_CODE IN (1,2,3) ");
            }
        }
        else {
            // Insert, Update, Delete, SelectLob, LobWrite, LobTrim, and LobErase
            if (false) {
                query.append(") OR ((OPERATION_CODE IN (1,2,3,9,10,11,29) ");
            }
            else {
                query.append(") OR ((OPERATION_CODE IN (1,2,3) ");
            }
            // In this mode, the connector will filter DDL operations based on the table inclusion/exclusion lists
            query.append("OR ").append(buildDdlPredicate()).append(") ");
        }

        // Always ignore the flush table
        query.append("AND TABLE_NAME != '").append(LOGMNR_FLUSH_TABLE).append("' ");

        // There are some common schemas that we automatically ignore when building the runtime Filter
        // predicates and we put that same list of schemas here and apply those in the generated SQL.
        if (!EXCLUDED_SCHEMAS.isEmpty()) {
            query.append("AND SEG_OWNER NOT IN (");
            for (Iterator<String> i = EXCLUDED_SCHEMAS.iterator(); i.hasNext();) {
                String excludedSchema = i.next();
                query.append("'").append(excludedSchema.toUpperCase()).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(") ");
        }

        String schemaPredicate = "SEG_OWNER = 'TY'";
        if (!Strings.isNullOrEmpty(schemaPredicate)) {
            query.append("AND ").append(schemaPredicate).append(" ");
        }

        String tablePredicate = "TABLE_NAME = 'ty_bigdata_4'";
        if (!Strings.isNullOrEmpty(tablePredicate)) {
            query.append("AND ").append(tablePredicate).append(" ");
        }

        query.append("))");

        Set<String> excludedUsers = new HashSet<>();
        if (!excludedUsers.isEmpty()) {
            query.append(" AND USERNAME NOT IN (");
            for (Iterator<String> i = excludedUsers.iterator(); i.hasNext();) {
                String user = i.next();
                query.append("'").append(user).append("'");
                if (i.hasNext()) {
                    query.append(",");
                }
            }
            query.append(")");
        }

        return query.toString();
    }

    private static String buildDdlPredicate() {
        final StringBuilder predicate = new StringBuilder(256);
        predicate.append("(OPERATION_CODE = 5 ");
        predicate.append("AND USERNAME NOT IN ('SYS','SYSTEM') ");
        predicate.append("AND INFO NOT LIKE 'INTERNAL DDL%' ");
        predicate.append("AND (TABLE_NAME IS NULL OR TABLE_NAME NOT LIKE 'ORA_TEMP_%'))");
        return predicate.toString();
    }

    public static String getSqlRedo(ResultSet rs) throws SQLException {
        int lobLimitCounter = 9; // todo : decide on approach (XStream chunk option) and Lob limit

        String redoSql = rs.getString(SQL_REDO);
        if (redoSql == null) {
            return null;
        }

        StringBuilder result = new StringBuilder(redoSql);
        int csf = rs.getInt(CSF);

        // 0 - indicates SQL_REDO is contained within the same row
        // 1 - indicates that either SQL_REDO is greater than 4000 bytes in size and is continued in
        // the next row returned by the ResultSet
        while (csf == 1) {
            rs.next();
            if (lobLimitCounter-- == 0) {
                LOGGER.warn("LOB value was truncated due to the connector limitation of {} MB", 40);
                break;
            }

            redoSql = rs.getString(SQL_REDO);
            result.append(redoSql);
            csf = rs.getInt(CSF);
        }

        return result.toString();
    }

    public static String showSGA() {
        return "SELECT * FROM V$SGA";
    }
}

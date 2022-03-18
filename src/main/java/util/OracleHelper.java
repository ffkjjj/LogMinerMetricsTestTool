package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

/**
 * @author zhul
 * @date 2022/3/18 13:24
 * @description
 */
public class OracleHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleHelper.class);

    public static void printSGA(Connection conn) {
        try (ResultSet rs = conn.createStatement().executeQuery(SqlUtils.showSGA())) {
            DecimalFormat df = new DecimalFormat("#,###");
            LOGGER.info("SGA: ");
            while (rs.next()) {
                String mb = df.format(rs.getLong(2) / (1024 * 1024));
                LOGGER.info("{}: {} MB", rs.getString(1), mb);
            }
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    public static void printMetrics(Connection conn, String name) {
        String sql = "SELECT VALUE FROM v$statname n, v$mystat m WHERE n.name=? AND n.statistic#=m.statistic#";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                DecimalFormat df = new DecimalFormat("#,###");
                while (rs.next()) {
                    LOGGER.info("{}: {} MB", name, df.format(rs.getLong(1) / (1024 * 1024)));
                }
            }
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }

    public static void printPGA(Connection connection) {
        String sql = " SELECT a.PGA_USED_MEM,a.PGA_ALLOC_MEM,a.PGA_FREEABLE_MEM,a.PGA_MAX_MEM FROM V$PROCESS a WHERE ADDR IN (SELECT PADDR FROM V$SESSION WHERE SID = (SELECT DISTINCT SID FROM V$MYSTAT))";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                DecimalFormat df = new DecimalFormat("#,###");
                while (rs.next()) {
                    LOGGER.info("PGA_USED_MEM: {} MB", df.format(rs.getLong(1) / (1024 * 1024)));
                    LOGGER.info("PGA_ALLOC_MEM: {} MB", df.format(rs.getLong(2) / (1024 * 1024)));
                    LOGGER.info("PGA_FREEABLE_MEM: {} MB", df.format(rs.getLong(3) / (1024 * 1024)));
                    LOGGER.info("PGA_MAX_MEM: {} MB", df.format(rs.getLong(4) / (1024 * 1024)));
                }
            }
        } catch (SQLException e) {
            LOGGER.error("", e);
        }
    }
}

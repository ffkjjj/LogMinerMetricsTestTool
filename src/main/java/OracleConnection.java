import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author zhul
 */
public class OracleConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(OracleConnection.class);

    private final String url;

    private final String userName;

    private final String password;

    private Connection connection;

    public OracleConnection(String url, String userName, String password) throws SQLException {
        this.url = url;
        this.userName = userName;
        this.password = password;

        this.connection = DriverManager.getConnection(url, userName, password);
    }

    public Connection connection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            LOGGER.info("reconnect...");
            this.connection = DriverManager.getConnection(url, userName, password);
        }
        return connection;
    }

    public OracleConnection executeWithoutCommitting(String... statements) throws SQLException {
        Connection conn = connection();
        try (Statement statement = conn.createStatement()) {
            for (String stmt : statements) {
                LOGGER.debug("executing sql: {}", stmt);
                statement.execute(stmt);
            }
        }
        return this;
    }

    public OracleConnection query(String query, ResultSetConsumer resultConsumer) throws SQLException {
        return query(query, Connection::createStatement, resultConsumer);
    }

    public OracleConnection query(String query, StatementFactory statementFactory, ResultSetConsumer resultConsumer) throws SQLException {
        Connection conn = connection();
        try (Statement statement = statementFactory.createStatement(conn)) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.debug("running '{}'", query);
            }
            try (ResultSet resultSet = statement.executeQuery(query);) {
                if (resultConsumer != null) {
                    resultConsumer.accept(resultSet);
                }
            }
        }
        return this;
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    public interface ResultSetConsumer {
        void accept(ResultSet rs) throws SQLException;
    }

    @FunctionalInterface
    public interface StatementFactory {
        /**
         * Use the given connection to create a statement.
         * @param connection the JDBC connection; never null
         * @return the statement
         * @throws SQLException if there are problems creating a statement
         */
        Statement createStatement(Connection connection) throws SQLException;
    }
}

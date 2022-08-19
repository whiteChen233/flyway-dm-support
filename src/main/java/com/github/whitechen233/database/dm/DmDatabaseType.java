package com.github.whitechen233.database.dm;

import java.sql.Connection;
import java.util.regex.Pattern;

import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.callback.CallbackExecutor;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;
import org.flywaydb.core.internal.sqlscript.SqlScriptExecutorFactory;

public class DmDatabaseType extends BaseDatabaseType {

    private static final Pattern USERNAME_PASSWORD_PATTERN = Pattern.compile(
        "^jdbc:dm:[a-zA-Z0-9#_$]+/([a-zA-Z0-9#_$]+)@.*");

    @Override
    public String getName() {
        return "DM";
    }

    @Override
    public int getNullType() {
        return 12;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:dm");
    }

    @Override
    public Pattern getJDBCCredentialsPattern() {
        return USERNAME_PASSWORD_PATTERN;
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return "dm.jdbc.driver.DmDriver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion,
                                                        Connection connection) {
        return databaseProductName.startsWith("DM");
    }

    @Override
    public Database<DmConnection> createDatabase(Configuration configuration,
                                                 JdbcConnectionFactory jdbcConnectionFactory,
                                                 StatementInterceptor statementInterceptor) {
        return new DmDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider,
                               ParsingContext parsingContext) {
        return new DmParser(configuration, parsingContext);
    }

    @Override
    public SqlScriptExecutorFactory createSqlScriptExecutorFactory(JdbcConnectionFactory jdbcConnectionFactory,
                                                                   final CallbackExecutor callbackExecutor,
                                                                   final StatementInterceptor statementInterceptor) {
        return (connection, undo, batch, outputQueryResults) -> new DmSqlScriptExecutor(
            new JdbcTemplate(connection, DmDatabaseType.this), callbackExecutor,
            undo, batch, outputQueryResults, statementInterceptor);
    }
}

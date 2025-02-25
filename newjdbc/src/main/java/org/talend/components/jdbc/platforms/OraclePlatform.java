/*
 * Copyright (C) 2006-2024 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.platforms;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.common.JDBCConfiguration;
import org.talend.components.jdbc.schema.Dbms;
import org.talend.components.jdbc.service.I18nMessage;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;

/**
 * https://docs.oracle.com/cd/B28359_01/server.111/b28310/tables003.htm#ADMIN01503
 */
@Slf4j
public class OraclePlatform extends Platform {

    public static final String ORACLE = "oracle";

    /*
     * https://docs.oracle.com/cd/B14117_01/server.101/b10758/sqlqr06.htm
     */
    private static final String VARCHAR2_MAX = "4000";

    @Override
    protected String buildUrlFromPattern(final String protocol, final String host, final int port,
            final String database,
            String params) {
        if (!params.trim().isEmpty()) {
            params = "?" + params;
        }
        return String.format("%s:@%s:%s:%s%s", protocol, host, port, database, params);
    }

    public OraclePlatform(final I18nMessage i18n, final JDBCConfiguration.Driver driver) {
        super(i18n, driver);
    }

    @Override
    public String name() {
        return ORACLE;
    }

    @Override
    public String delimiterToken() {
        return "\"";
    }

    @Override
    protected String buildQuery(final Connection connection, final Table table, final boolean useOriginColumnName,
            Dbms mapping)
            throws SQLException {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(table.getSchema()).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        sql.append(createColumns(table.getColumns(), useOriginColumnName, mapping));
        sql
                .append(createPKs(connection.getMetaData(), table.getName(),
                        table.getColumns().stream().filter(Column::isPrimaryKey).collect(Collectors.toList())));
        // todo create index
        sql.append(")");

        log.debug("### create table query ###");
        log.debug(sql.toString());
        return sql.toString();
    }

    @Override
    public void addDataSourceProperties(final HikariDataSource dataSource) {
        super.addDataSourceProperties(dataSource);
        dataSource.addDataSourceProperty("oracle.jdbc.J2EE13Compliant", "TRUE");
    }

    @Override
    protected boolean isTableExistsCreationError(final Throwable e) {
        return e instanceof SQLException && "42000".equals(((SQLException) e).getSQLState())
                && ((SQLException) e).getErrorCode() == 955;
    }

}

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
package org.talend.components.jdbc.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.ReturnVariables.ReturnVariable;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.context.RuntimeContext;
import org.talend.sdk.component.api.context.RuntimeContextHolder;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.SQLException;

import static org.talend.sdk.component.api.component.ReturnVariables.ReturnVariable.AVAILABILITY.AFTER;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCInput")
@ReturnVariable(value = "QUERY", availability = AFTER, type = String.class)
@Emitter(name = "Input")
@Documentation("JDBC query input")
public class QueryEmitter implements Serializable {

    private static final long serialVersionUID = 1;

    private final JDBCInputConfig configuration;

    private final RecordBuilderFactory recordBuilderFactory;

    private final JDBCService jdbcService;

    @RuntimeContext
    private transient RuntimeContextHolder context;

    @Connection
    private transient java.sql.Connection connection;

    private transient JDBCService.DataSourceWrapper dataSource;

    // private final I18nMessage i18n;

    private transient JDBCInputReader reader;

    public QueryEmitter(@Option("configuration") final JDBCInputConfig configuration, final JDBCService jdbcService,
            final RecordBuilderFactory recordBuilderFactory/* .final I18nMessage i18nMessage */) {
        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcService = jdbcService;
        // this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() throws SQLException {
        if (context != null) {
            context.set("QUERY", configuration.getDataSet().getSqlQuery());
        }

        boolean useExistedConnection = false;
        if (connection == null) {
            dataSource = jdbcService.createConnectionOrGetFromSharedConnectionPoolOrDataSource(
                    configuration.getDataSet().getDataStore(), context, false);
        } else {
            useExistedConnection = true;
            dataSource = new JDBCService.DataSourceWrapper(null, connection);
        }

        reader = new JDBCInputReader(configuration, jdbcService, useExistedConnection, dataSource, recordBuilderFactory,
                context);
        reader.open();
    }

    @Producer
    public Record next() throws SQLException {
        if (reader.advance()) {
            return reader.getCurrent();
        } else {
            return null;
        }
    }

    @PreDestroy
    public void release() throws SQLException {
        if (reader != null) {
            reader.close();
        }
    }

}

/*
 * Copyright (C) 2006-2023 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.output;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.ReturnVariables.ReturnVariable;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.context.RuntimeContext;
import org.talend.sdk.component.api.context.RuntimeContextHolder;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.connection.Connection;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

import static org.talend.sdk.component.api.component.ReturnVariables.ReturnVariable.AVAILABILITY.AFTER;

@Slf4j
@Getter
@Version(1)
@ReturnVariable(value = "NB_LINE_INSERTED", availability = AFTER, type = Integer.class)
@ReturnVariable(value = "NB_LINE_UPDATED", availability = AFTER, type = Integer.class)
@ReturnVariable(value = "NB_LINE_DELETED", availability = AFTER, type = Integer.class)
@ReturnVariable(value = "NB_LINE_REJECTED", availability = AFTER, type = Integer.class)
@ReturnVariable(value = "QUERY", availability = AFTER, type = String.class)
@Processor(name = "Output")
@Icon(value = Icon.IconType.CUSTOM, custom = "datastore-connector")
@Documentation("JDBC Output component")
public class OutputProcessor implements Serializable {

    private static final long serialVersionUID = 1;

    private final JDBCOutputConfig configuration;

    private final RecordBuilderFactory recordBuilderFactory;

    private final JDBCService jdbcService;

    // private final I18nMessage i18n;

    @RuntimeContext
    private transient RuntimeContextHolder context;

    @Connection
    private transient java.sql.Connection connection;

    private transient JDBCService.DataSourceWrapper dataSource;

    private transient boolean init;

    private transient JDBCOutputWriter writer;

    public OutputProcessor(@Option("configuration") final JDBCOutputConfig configuration,
            final JDBCService jdbcService, final RecordBuilderFactory recordBuilderFactory/*
                                                                                           * , final I18nMessage
                                                                                           * i18nMessage
                                                                                           */) {
        this.configuration = configuration;
        this.jdbcService = jdbcService;
        this.recordBuilderFactory = recordBuilderFactory;
        // this.i18n = i18nMessage;
    }

    @ElementListener
    public void elementListener(@Input final Record record, @Output final OutputEmitter<Record> success,
            @Output("reject") final OutputEmitter<Record> reject)
            throws SQLException {
        if (!init) {
            boolean useExistedConnection = false;

            if (connection == null) {
                try {
                    dataSource = jdbcService.createConnectionOrGetFromSharedConnectionPoolOrDataSource(
                            configuration.getDataSet().getDataStore(), context, false);

                    if (configuration.getCommitEvery() != 0) {
                        dataSource.getConnection().setAutoCommit(false);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            } else {
                useExistedConnection = true;
                dataSource = new JDBCService.DataSourceWrapper(null, connection);
            }

            switch (configuration.getDataAction()) {
            case INSERT:
                writer = new JDBCOutputInsertWriter(configuration, useExistedConnection, dataSource,
                        recordBuilderFactory, context);
                break;
            case UPDATE:
                writer = new JDBCOutputUpdateWriter(configuration, useExistedConnection, dataSource,
                        recordBuilderFactory, context);
                break;
            case INSERT_OR_UPDATE:
                writer = new JDBCOutputInsertOrUpdateWriter(configuration, useExistedConnection, dataSource,
                        recordBuilderFactory, context);
                break;
            case UPDATE_OR_INSERT:
                writer = new JDBCOutputUpdateOrInsertWriter(configuration, useExistedConnection, dataSource,
                        recordBuilderFactory, context);
                break;
            case DELETE:
                writer = new JDBCOutputDeleteWriter(configuration, useExistedConnection, dataSource,
                        recordBuilderFactory, context);
                break;
            }

            writer.open();

            init = true;
        }

        // as output component, it's impossible that record is null
        if (record == null) {
            return;
        }

        writer.write(record);

        List<Record> successfulWrites = writer.getSuccessfulWrites();
        for (Record r : successfulWrites) {
            success.emit(r);
        }

        List<Record> rejectedWrites = writer.getRejectedWrites();
        for (Record r : rejectedWrites) {
            reject.emit(r);
        }
    }

    @PostConstruct
    public void init() {
    }

    @PreDestroy
    public void release() throws SQLException {
        if (writer != null) {
            writer.close();
        }
    }

}

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
package org.talend.components.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.jdbc.datastore.JDBCDataStore;
import org.talend.components.jdbc.rollback.JDBCRollbackConfig;
import org.talend.components.jdbc.rollback.JDBCRollbackProcessor;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.jdbc")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of JDBC rollback component")
class JDBCRollbackTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private JDBCService jdbcService;

    private JDBCDataStore dataStore;

    @BeforeAll
    public void beforeAll() throws Exception {
        dataStore = DBTestUtils.createDataStore(false);
    }

    @Test
    void testRollback() throws SQLException {
        JDBCRollbackConfig config = new JDBCRollbackConfig();

        JDBCRollbackProcessor processor = new JDBCRollbackProcessor(config, jdbcService, recordBuilderFactory);
        try (JDBCService.DataSourceWrapper dataSourceWrapper = jdbcService.createDataSource(dataStore)) {
            processor.doRollback(dataSourceWrapper.getConnection());
            assertTrue(dataSourceWrapper.getConnection().isClosed());
        }
    }

    @Test
    void testClose() throws SQLException {
        JDBCRollbackConfig config = new JDBCRollbackConfig();
        config.setClose(false);

        JDBCRollbackProcessor processor = new JDBCRollbackProcessor(config, jdbcService, recordBuilderFactory);
        try (JDBCService.DataSourceWrapper dataSourceWrapper = jdbcService.createDataSource(dataStore)) {
            processor.doRollback(dataSourceWrapper.getConnection());
            assertFalse(dataSourceWrapper.getConnection().isClosed());
        }
    }

}

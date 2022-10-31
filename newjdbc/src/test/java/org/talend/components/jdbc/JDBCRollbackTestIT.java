//============================================================================
//
// Copyright (C) 2006-2022 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
//============================================================================
package org.talend.components.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.jdbc.commit.JDBCCommitConfig;
import org.talend.components.jdbc.commit.JDBCCommitProcessor;
import org.talend.components.jdbc.datastore.JDBCDataStore;
import org.talend.components.jdbc.rollback.JDBCRollbackConfig;
import org.talend.components.jdbc.rollback.JDBCRollbackProcessor;
import org.talend.components.jdbc.service.JDBCService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.jdbc")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of JDBC rollback component")
public class JDBCRollbackTestIT {

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
    public void testRollback() throws SQLException {
        JDBCRollbackConfig config = new JDBCRollbackConfig();

        JDBCRollbackProcessor processor = new JDBCRollbackProcessor(config, jdbcService, recordBuilderFactory);
        try(JDBCService.DataSourceWrapper dataSourceWrapper = jdbcService.createConnection(dataStore)) {
            processor.doRollback(dataSourceWrapper);
            assertTrue(dataSourceWrapper.getConnection().isClosed());
        }
    }

    @Test
    public void testClose() throws SQLException {
        JDBCRollbackConfig config = new JDBCRollbackConfig();
        config.setClose(false);

        JDBCRollbackProcessor processor = new JDBCRollbackProcessor(config, jdbcService, recordBuilderFactory);
        try(JDBCService.DataSourceWrapper dataSourceWrapper = jdbcService.createConnection(dataStore)) {
            processor.doRollback(dataSourceWrapper);
            assertTrue(!dataSourceWrapper.getConnection().isClosed());
        }
    }

}

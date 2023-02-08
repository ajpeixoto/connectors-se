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
package org.talend.components.jdbc.datastore;

import lombok.Data;
import lombok.ToString;
import org.talend.components.jdbc.common.Driver;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.condition.UIScope;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.AND;

@Data
@ToString(exclude = { "password" })
@GridLayout({
        @GridLayout.Row("jdbcUrl"),
        @GridLayout.Row("jdbcDriver"),
        @GridLayout.Row("jdbcClass"),
        @GridLayout.Row("userId"),
        @GridLayout.Row("password"),
        @GridLayout.Row("useSharedDBConnection"),
        @GridLayout.Row("sharedDBConnectionName"),
        @GridLayout.Row("useDataSource"),
        @GridLayout.Row("dataSourceAlias"),
        @GridLayout.Row("dbMapping")
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row("useAutoCommit"),
        @GridLayout.Row("autoCommit")
})
@DataStore("JDBCDataStore")
@Checkable("CheckConnection")
@Documentation("A connection to a database")
public class JDBCDataStore implements Serializable {

    @Option
    @Documentation("jdbc url")
    private String jdbcUrl = "jdbc:";

    @Option
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_SCOPE })
    @Documentation("jdbc driver table")
    private List<Driver> jdbcDriver = Collections.emptyList();

    @Option
    @Suggestable(value = "GUESS_DRIVER_CLASS", parameters = { "jdbcDriver" })
    @Documentation("driver class")
    private String jdbcClass;

    @Option
    @Documentation("database user")
    private String userId;

    @Option
    @Credential
    @Documentation("database password")
    private String password;

    @Option
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_CONNECTION_COMPONENT_SCOPE })
    @Documentation("use or register a shared DB connection")
    private boolean useSharedDBConnection;

    @Option
    @ActiveIfs(operator = AND, value = { @ActiveIf(target = "useSharedDBConnection", value = { "true" }),
            @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_CONNECTION_COMPONENT_SCOPE }) })
    @Documentation("shared DB connection name for register or fetch")
    private String sharedDBConnectionName;

    @Option
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_COMPONENT_SCOPE })
    @Documentation("use data source")
    private boolean useDataSource;

    @Option
    @ActiveIfs(operator = AND, value = { @ActiveIf(target = "useDataSource", value = { "true" }),
            @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_COMPONENT_SCOPE }) })
    @Documentation("data source alias for fetch")
    private String dataSourceAlias;

    // TODO map to studio "widget.type.mappingType" like tcompv0
    @Option
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_METADATA_SCOPE })
    @Documentation("select db mapping file for type convert")
    private String dbMapping;

    // advanced setting

    @Option
    @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_CONNECTION_COMPONENT_SCOPE })
    @Documentation("decide if call auto commit method")
    private boolean useAutoCommit = true;

    @Option
    @ActiveIfs(operator = AND, value = { @ActiveIf(target = "useAutoCommit", value = { "true" }),
            @ActiveIf(target = UIScope.TARGET, value = { UIScope.STUDIO_CONNECTION_COMPONENT_SCOPE }) })
    @Documentation("if true, mean auto commit, else disable auto commit, as different database, default auto commit value is different")
    private boolean autoCommit;

    public String getJdbcUrl() {
        return jdbcUrl == null ? null : jdbcUrl.trim();
    }

}

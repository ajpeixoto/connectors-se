/*
 * Copyright (C) 2006-2022 Talend Inc. - www.talend.com
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

import lombok.Data;
import org.talend.components.jdbc.common.DBType;
import org.talend.components.jdbc.common.PreparedStatementParameter;
import org.talend.components.jdbc.dataset.JDBCQueryDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@Data
@GridLayout({
        @GridLayout.Row("dataSet")
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row("dataSet"),
        @GridLayout.Row("useCursor"),
        @GridLayout.Row("cursorSize"),
        @GridLayout.Row("trimAllStringOrCharColumns"),
        @GridLayout.Row("columnTrims"),
        @GridLayout.Row("enableMapping"),
        @GridLayout.Row("mapping"),
        @GridLayout.Row("allowSpecialChar"),
        @GridLayout.Row("usePreparedStatement"),
        @GridLayout.Row("preparedStatementParameters"),
        @GridLayout.Row("useQueryTimeout"),
        @GridLayout.Row("queryTimeout")
})
@Documentation("jdbc input")
public class JDBCInputConfig implements Serializable {

    @Option
    @Documentation("SQL query dataset")
    private JDBCQueryDataSet dataSet;

    // advanced setting

    @Option
    @Documentation("use cursor")
    private boolean useCursor;

    @Option
    @ActiveIf(target = "useCursor", value = { "true" })
    @Documentation("cursor size")
    private int cursorSize = 1000;

    @Option
    @Documentation("trim all columns")
    private boolean trimAllStringOrCharColumns;

    @Option
    @ActiveIf(target = "trimAllStringOrCharColumns", value = { "false" })
    @Documentation("")
    private List<ColumnTrim> columnTrims;

    @Option
    @Documentation("enable mapping")
    private boolean enableMapping;

    // TODO use enum or a new widget mapping? "widget.type.mappingType":"MAPPING_TYPE"
    // TODO duplicated with the one in datastore for metadata though
    @Option
    @ActiveIf(target = "enableMapping", value = { "true" })
    @Documentation("select DB mapping")
    // private String mapping;
    private DBType mapping = DBType.MYSQL;

    // TODO what's this for runtime?
    @Option
    @Documentation("")
    private boolean allowSpecialChar = true;

    @Option
    @Documentation("")
    private boolean usePreparedStatement;

    @Option
    @ActiveIf(target = "usePreparedStatement", value = { "true" })
    @Documentation("")
    private List<PreparedStatementParameter> preparedStatementParameters;

    @Option
    @Documentation("")
    private boolean useQueryTimeout;

    @Option
    @ActiveIf(target = "useQueryTimeout", value = { "true" })
    @Documentation("")
    private int queryTimeout = 30;

}

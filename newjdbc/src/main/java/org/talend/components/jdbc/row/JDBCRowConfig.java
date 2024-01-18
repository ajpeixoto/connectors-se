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
package org.talend.components.jdbc.row;

import lombok.Data;
import org.talend.components.jdbc.common.PreparedStatementParameter;
import org.talend.components.jdbc.dataset.JDBCQueryDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.BuiltInSuggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@Data
@GridLayout({
        @GridLayout.Row("dataSet"),
        @GridLayout.Row("dieOnError")
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row("dataSet"),
        @GridLayout.Row("propagateRecordSet"),
        @GridLayout.Row("recordSetColumn"),
        @GridLayout.Row("usePreparedStatement"),
        @GridLayout.Row("preparedStatementParameters"),
        @GridLayout.Row("detectErrorWhenMultiStatements"),
        @GridLayout.Row("commitEvery"),
        @GridLayout.Row("useQueryTimeout"),
        @GridLayout.Row("queryTimeout")
})
@Documentation("Jdbc row.")
public class JDBCRowConfig implements Serializable {

    @Option
    @Documentation("Table dataset.")
    private JDBCQueryDataSet dataSet;

    @Option
    @Documentation("Undefined.")
    private boolean dieOnError;

    @Option
    @Documentation("Undefined.")
    private boolean propagateRecordSet;

    @BuiltInSuggestable(BuiltInSuggestable.Name.CURRENT_SCHEMA_ENTRY_NAMES)
    @Option
    @ActiveIf(target = "propagateRecordSet", value = { "true" })
    @Documentation("Undefined.")
    private String recordSetColumn;

    @Option
    @Documentation("Undefined.")
    private boolean usePreparedStatement;

    @Option
    @ActiveIf(target = "usePreparedStatement", value = { "true" })
    @Documentation("Undefined.")
    private List<PreparedStatementParameter> preparedStatementParameters;

    @Option
    @Documentation("Undefined.")
    private boolean detectErrorWhenMultiStatements;

    @Option
    @Documentation("Undefined.")
    private int commitEvery = 10000;

    @Option
    @Documentation("Undefined.")
    private boolean useQueryTimeout;

    @Option
    @ActiveIf(target = "useQueryTimeout", value = { "true" })
    @Documentation("Undefined.")
    private int queryTimeout = 30;
}

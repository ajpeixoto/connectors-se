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
package org.talend.components.jdbc.sp;

import lombok.Data;
import org.talend.components.jdbc.common.SchemaInfo;
import org.talend.components.jdbc.datastore.JDBCDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.BuiltInSuggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

@Data
@GridLayout({
        @GridLayout.Row("schema"),
        @GridLayout.Row("dataStore"),
        @GridLayout.Row("spName"),
        @GridLayout.Row("isFunction"),
        @GridLayout.Row("resultColumn"),
        @GridLayout.Row("spParameters")
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {
        @GridLayout.Row("dataStore")
})
@Documentation("JDBC SP config.")
public class JDBCSPConfig implements Serializable {

    @Option
    @Documentation("Schema dataset.")
    private JDBCDataStore dataStore;

    @Option
    @Structure(type = Structure.Type.OUT)
    @Documentation("Schema.")
    private List<SchemaInfo> schema;

    @Option
    @Documentation("SP Name.")
    private String spName;

    @Option
    @Documentation("Is Function.")
    private boolean isFunction;

    @Option
    @BuiltInSuggestable(BuiltInSuggestable.Name.CURRENT_SCHEMA_ENTRY_NAMES)
    @ActiveIf(target = "isFunction", value = { "true" })
    @Documentation("Result Column.")
    private String resultColumn;

    @Option
    @Documentation("SP Parameters.")
    private List<SPParameter> spParameters;

}

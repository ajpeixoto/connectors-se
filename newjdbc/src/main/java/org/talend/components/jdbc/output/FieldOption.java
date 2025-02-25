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
package org.talend.components.jdbc.output;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.widget.BasedOnSchema;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@OptionsOrder({ "columnName", "updateKey", "deleteKey", "updatable", "insertable" })
@NoArgsConstructor
@AllArgsConstructor
@Documentation("")
public class FieldOption implements Serializable {

    @BasedOnSchema
    @Option
    @Documentation("")
    private String columnName;

    @Option
    @Documentation("")
    private boolean updateKey;

    @Option
    @Documentation("")
    private boolean deleteKey;

    @Option
    @Documentation("")
    private boolean updatable = true;

    @Option
    @Documentation("")
    private boolean insertable = true;

}

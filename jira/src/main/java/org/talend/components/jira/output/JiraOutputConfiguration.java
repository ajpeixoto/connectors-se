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
package org.talend.components.jira.output;

import java.io.Serializable;

import org.talend.components.jira.dataset.JiraDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("dataset"), @GridLayout.Row("outputAction") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("dataset"),
        @GridLayout.Row("deleteSubtasks") })
public class JiraOutputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset.")
    private JiraDataset dataset;

    @Option
    @DefaultValue("CREATE")
    @Documentation("Output action.")
    private OutputAction outputAction = OutputAction.CREATE;

    @Option
    @DefaultValue("true")
    @ActiveIfs({ @ActiveIf(target = "../dataset.resourceType", value = "ISSUE"),
            @ActiveIf(target = "outputAction", value = "DELETE") })
    @Documentation("If the issue has no subtasks this parameter is ignored. "
            + "If the issue has subtasks and this parameter is false, "
            + "then the issue will not be deleted and an error will be returned.")
    private boolean deleteSubtasks = true;
}

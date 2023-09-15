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
package org.talend.components.jira.source;

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
@GridLayout({ @GridLayout.Row("dataset"), @GridLayout.Row("useJQL"), @GridLayout.Row("jql"),
        @GridLayout.Row("projectId"), @GridLayout.Row("issueId") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("dataset"), @GridLayout.Row("batchSize") })
public class JiraInputConfiguration implements Serializable {

    @Option
    @Documentation("Dataset.")
    private JiraDataset dataset;

    @Option
    @ActiveIf(target = "../dataset.resourceType", value = "PROJECT")
    @Documentation("Project id.")
    private String projectId;

    @Option
    @DefaultValue("true")
    @ActiveIf(target = "../dataset.resourceType", value = "ISSUE")
    @Documentation("Use JQL query.")
    private boolean useJQL = true;

    @Option
    @ActiveIfs({ @ActiveIf(target = "../dataset.resourceType", value = "ISSUE"),
            @ActiveIf(target = "useJQL", value = "true") })
    @DefaultValue("summary ~ \"some word\" AND project=PROJECT_ID")
    @Documentation("JQL query string.")
    private String jql = "summary ~ \"some word\" AND project=PROJECT_ID";

    @Option
    @ActiveIfs({ @ActiveIf(target = "../dataset.resourceType", value = "ISSUE"),
            @ActiveIf(target = "useJQL", value = "false") })
    @Documentation("Issue id.")
    private String issueId;

    @Option
    @ActiveIfs({ @ActiveIf(target = "../dataset.resourceType", value = "ISSUE"),
            @ActiveIf(target = "useJQL", value = "true") })
    @DefaultValue("50")
    @Documentation("Batch size.")
    private int batchSize = 50;
}

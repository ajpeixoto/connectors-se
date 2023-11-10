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
package org.talend.components.jira.datastore;

import java.io.Serializable;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;
import lombok.Data;

@Data
@GridLayout(value = { @GridLayout.Row("jiraURL"), @GridLayout.Row("authenticationType"), @GridLayout.Row("pat"),
        @GridLayout.Row({ "user", "pass" }) })
@DataStore("Jira")
public class JiraDatastore implements Serializable {

    @Option
    @DefaultValue("https://jira.atlassian.com")
    @Documentation("Jira base url.")
    private String jiraURL = "https://jira.atlassian.com";

    @Option
    @Documentation("Jira authentication.")
    @DefaultValue("BASIC")
    private AuthenticationType authenticationType = AuthenticationType.BASIC;

    @Option
    @ActiveIf(target = "authenticationType", value = "BASIC")
    @Documentation("User.")
    private String user;

    @Option
    @Credential
    @ActiveIf(target = "authenticationType", value = "BASIC")
    @Documentation("Password.")
    private String pass;

    @Option
    @Credential
    @ActiveIf(target = "authenticationType", value = "PAT")
    @Documentation("Personal access token value.")
    private String pat;

    public enum AuthenticationType {
        BASIC,
        PAT
    }

}

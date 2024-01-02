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
package org.talend.components.dynamicscrm.datastore;

import static org.talend.components.dynamicscrm.service.UIActionService.ACTION_HEALTHCHECK_DYNAMICS365;
import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

import java.io.Serializable;

import org.talend.components.dynamicscrm.migration.DynamicsConnectionMigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@Checkable(ACTION_HEALTHCHECK_DYNAMICS365)
@DataStore("DynamicsCrmConnection")
@Version(value = 2, migrationHandler = DynamicsConnectionMigrationHandler.class)
@GridLayout({
        @GridLayout.Row({ "instance" }),
        @GridLayout.Row({ "opDomain", "opHost" }),
        @GridLayout.Row({ "opUserName", "opPassword" }),
        @GridLayout.Row({ "appType" }),
        @GridLayout.Row({ "flow" }),
        @GridLayout.Row({ "username", "password" }),
        @GridLayout.Row({ "serviceRootUrl" }),
        @GridLayout.Row({ "clientId" }), @GridLayout.Row({ "clientSecret" }),
        @GridLayout.Row({ "authorizationEndpoint" }) })
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("timeout"), @GridLayout.Row("maxRetries") })
@Documentation("Dynamics CRM connection")
public class DynamicsCrmConnection implements Serializable {

    @Option
    @Required
    @DefaultValue("CLOUD")
    @Documentation("Select if you want to query cloud or on-premise instance.")
    private Instance instance = Instance.CLOUD;

    @Option
    @Required
    @DefaultValue("")
    @ActiveIf(target = "instance", value = "ON_PREMISE")
    @Documentation("On-premise username.")
    private String opUserName = "";

    @Option
    @Required
    @DefaultValue("")
    @ActiveIf(target = "instance", value = "ON_PREMISE")
    @Documentation("On-premise password.")
    private String opPassword = "";

    @Option
    @Required
    @DefaultValue("")
    @ActiveIf(target = "instance", value = "ON_PREMISE")
    @Documentation("On-premise domain.")
    private String opDomain = "";

    @Option
    @Required
    @DefaultValue("")
    @ActiveIf(target = "instance", value = "ON_PREMISE")
    @Documentation("On-premise host.")
    private String opHost = "";

    @Option
    @Required
    @DefaultValue("NATIVE")
    @ActiveIf(target = "instance", value = "CLOUD")
    @Documentation("Select the type of your application, either Native App or Web App with delegated permissions.")
    private AppType appType = AppType.NATIVE;

    @Option
    @DefaultValue("ROPC")
    @Documentation("Select which OAuth flow to use.")
    @ActiveIfs({
            @ActiveIf(target = "appType", value = "WEB"),
            @ActiveIf(target = "instance", value = "CLOUD")
    })
    private OAuthFlow flow = OAuthFlow.ROPC;

    @Option
    @Required
    @DefaultValue("")
    @Documentation("User name.")
    @ActiveIfs({
            @ActiveIf(target = "flow", value = "ROPC"),
            @ActiveIf(target = "instance", value = "CLOUD")
    })
    private String username = "";

    @Option
    @Required
    @Credential
    @DefaultValue("")
    @Documentation("Password.")
    @ActiveIfs({
            @ActiveIf(target = "flow", value = "ROPC"),
            @ActiveIf(target = "instance", value = "CLOUD")
    })
    private String password = "";

    @Option
    @Required
    @Documentation("Service Root URL")
    private String serviceRootUrl;

    @Option
    @Required
    @ActiveIf(target = "instance", value = "CLOUD")
    @Documentation("Client ID")
    private String clientId;

    @Option
    @Credential
    @ActiveIfs({
            @ActiveIf(target = "appType", value = "WEB"),
            @ActiveIf(target = "instance", value = "CLOUD")
    })
    @Documentation("Client secret")
    private String clientSecret;

    @Option
    @Required
    @ActiveIf(target = "instance", value = "CLOUD")
    @Documentation("OAuth authorization endpoint")
    private String authorizationEndpoint;

    @Option
    @Required
    @ActiveIf(target = "instance", value = "CLOUD")
    @Documentation("Timeout in seconds")
    private Integer timeout = 60;

    @Option
    @Required
    @Documentation("Max retries")
    private Integer maxRetries = 5;

}
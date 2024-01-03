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
package org.talend.components.http.configuration.auth;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@GridLayout(value = { @GridLayout.Row({ "flow", "authenticationType" }),
        @GridLayout.Row({ "tokenEndpoint" }),
        @GridLayout.Row({ "clientId", "clientSecret" }),
        @GridLayout.Row({ "scopes" })
})
@Documentation("OAuth 2.0 support.")
public class OAuth20 implements Serializable {

    @Option
    @Documentation("OAUTH flow.")
    private Authorization.OAuth20Flow flow = Authorization.OAuth20Flow.CLIENT_CREDENTIAL;

    @Option
    @Documentation("Authentication type within OAUTH2.0.")
    @ActiveIf(target = "flow", value = { "CLIENT_CREDENTIAL" })
    private Authorization.OAuth20Authent authenticationType = Authorization.OAuth20Authent.FORM;

    @Option
    @Pattern("^https?://.+$")
    @Documentation("The service provider's token endpoint.")
    private String tokenEndpoint;

    @Option
    @Documentation("The client ID.")
    private String clientId;

    @Option
    @Credential
    @Documentation("The client secret.")
    private String clientSecret;

    @Option
    @Documentation("The required scopes.")
    private List<String> scopes = new ArrayList<>();

}

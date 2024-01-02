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
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout({ @GridLayout.Row({ "type" }), @GridLayout.Row({ "basic" }), @GridLayout.Row({ "ntlm" }),
        @GridLayout.Row({ "bearerToken" }), @GridLayout.Row({ "apiKey" }),
        @GridLayout.Row({ "oauth20" }) })
@Documentation("Http authentication data store.")
public class Authentication implements Serializable {

    @Option
    @Documentation("Request authentication type.")
    private Authorization.AuthorizationType type = Authorization.AuthorizationType.NoAuth;

    @Option
    @ActiveIf(target = "type", value = { "Basic", "Digest" })
    @Documentation("Login/password authentication.")
    private Basic basic = new Basic();

    @Option
    @ActiveIf(target = "type", value = "NTLM")
    @Documentation("NTLM authentication.")
    private Ntlm ntlm = new Ntlm();

    @Option
    @Credential
    @ActiveIf(target = "type", value = "Bearer")
    @Documentation("Bearer token.")
    private String bearerToken;

    @Option
    @ActiveIf(target = "type", value = "APIKey")
    @Documentation("The API key configuration.")
    private APIKey apiKey;

    @Option
    @ActiveIf(target = "type", value = "OAuth20")
    @Documentation("The OAuth2.0 configuration.")
    private OAuth20 oauth20;

}

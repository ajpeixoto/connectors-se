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
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout(value = { @GridLayout.Row({ "destination", "headerName", "queryName" }),
        @GridLayout.Row({ "prefix", "token" }) })
@Documentation("API Key support.")
public class APIKey implements Serializable {

    @Option
    @Documentation("Where should be set the api key.")
    @DefaultValue("HEADERS")
    private Authorization.Destination destination = Authorization.Destination.HEADERS;

    @Option
    @DefaultValue("Authorization")
    @ActiveIf(target = "destination", value = "HEADERS")
    @Documentation("The name of the header parameter.")
    private String headerName;

    @Option
    @DefaultValue("apikey")
    @ActiveIf(target = "destination", value = "QUERY_PARAMETERS")
    @Documentation("The name of the query parameter.")
    private String queryName;

    @Option
    @DefaultValue("Bearer")
    @ActiveIf(target = "destination", value = "HEADERS")
    @Documentation("The prefix if any.")
    private String prefix;

    @Option
    @Credential
    @Documentation("The token value.")
    private String token;

}

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
package org.talend.components.http.configuration;

import java.io.Serializable;

import org.talend.components.http.configuration.auth.Authentication;
import org.talend.components.http.configuration.proxy.ProxyConfig;
import org.talend.components.http.migration.HttpClientDatastoreMigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@Version(value = Datastore.VERSION, migrationHandler = HttpClientDatastoreMigrationHandler.class)
@DataStore("Datastore")
@GridLayout({ @GridLayout.Row({ "base" }), //
        @GridLayout.Row({ "authentication" }) //
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "connectionTimeout" }),
        @GridLayout.Row({ "receiveTimeout" }),
        @GridLayout.Row({ "bypassCertificateValidation" }),
        @GridLayout.Row({ "useProxy" }),
        @GridLayout.Row({ "proxyConfiguration" })
})
@Documentation("HTTP connection configuration.")
public class Datastore implements Serializable {

    public static final int VERSION = 2;

    @Option
    @Required
    @Pattern("^https?://.+$")
    @Documentation("URL base of the request.")
    private String base = "";

    @Option
    @Required
    @Documentation("Authentication configuration.")
    private Authentication authentication = new Authentication();

    @Min(0)
    @Option
    @Required
    @Documentation("Connection timeout (ms).")
    @DefaultValue("30000")
    private Integer connectionTimeout = 30000;

    @Min(0)
    @Option
    @Required
    @Documentation("Read timeout (ms).")
    @DefaultValue("120000")
    private Integer receiveTimeout = 120000;

    @Option
    @Documentation("Bypass server certificate client validation.")
    private boolean bypassCertificateValidation;

    @Option
    @Documentation("Use a proxy.")
    private boolean useProxy;

    @Option
    @ActiveIf(target = "useProxy", value = "true")
    @Documentation("Proxy configuration.")
    private ProxyConfig proxyConfiguration = new ProxyConfig();

}

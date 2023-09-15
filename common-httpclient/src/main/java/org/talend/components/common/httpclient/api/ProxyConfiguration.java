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
package org.talend.components.common.httpclient.api;

import lombok.Data;
import org.talend.components.common.httpclient.api.authentication.LoginPassword;

@Data
public class ProxyConfiguration {

    public enum ProxyType {
        HTTP,
        SOCKS
    }

    public ProxyConfiguration(ProxyType type, String host, int port, String login, String password) {
        this.type = type;
        this.host = host;
        this.port = port;
        this.credentials = new LoginPassword(login, password);
    }

    private ProxyType type;

    private String host;

    private int port;

    private LoginPassword credentials;

}

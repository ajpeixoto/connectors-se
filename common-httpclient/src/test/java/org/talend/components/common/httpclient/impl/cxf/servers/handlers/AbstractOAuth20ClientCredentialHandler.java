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
package org.talend.components.common.httpclient.impl.cxf.servers.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.talend.components.common.httpclient.api.authentication.OAuth20;
import org.talend.components.common.httpclient.impl.cxf.ResourcesUtils;
import org.talend.components.common.httpclient.impl.cxf.servers.BasicHTTPServerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class AbstractOAuth20ClientCredentialHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        Map<String, String> params = getParams(exchange);

        String clientId = params.get(OAuth20.Keys.client_id.name());
        String clientSecret = params.get(OAuth20.Keys.client_secret.name());
        String grantType = params.get(OAuth20.Keys.grant_type.name());

        int status = HttpURLConnection.HTTP_BAD_REQUEST;
        String content = "";
        if ("POST".equals(exchange.getRequestMethod())
                && OAuth20.GrantType.client_credentials.name().equals(grantType)
                && BasicHTTPServerFactory.HTTP_OAUTH_CLIENT_CREDENTIALS_TOKEN_CLIENT_ID.equals(clientId)
                && BasicHTTPServerFactory.HTTP_OAUTH_CLIENT_CREDENTIALS_TOKEN_CLIENT_SECRET.equals(clientSecret)) {
            status = HttpURLConnection.HTTP_OK;
            content = ResourcesUtils.loadResource("/responses/oauth20TokenSuccess.json");
        } else {
            content = ResourcesUtils.loadResource("/responses/oauth20TokenFail.json");
        }

        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(bytes);
        os.close();
    }

    abstract Map<String, String> getParams(HttpExchange exchange);
}

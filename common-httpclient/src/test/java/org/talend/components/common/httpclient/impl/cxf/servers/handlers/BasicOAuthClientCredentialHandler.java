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

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cxf.helpers.HttpHeaderHelper;
import org.talend.components.common.httpclient.api.authentication.OAuth20;
import org.talend.components.common.httpclient.impl.cxf.servers.BasicHTTPServerFactory;

import com.sun.net.httpserver.HttpExchange;

public class BasicOAuthClientCredentialHandler extends AbstractOAuth20ClientCredentialHandler {

    @Override
    Map<String, String> getParams(HttpExchange exchange) {
        List<String> authorization = exchange.getRequestHeaders().get(HttpHeaderHelper.AUTHORIZATION);
        byte[] decode = Base64.getDecoder().decode(authorization.get(0).substring(6));
        String credentials = new String(decode);
        String[] split = credentials.split(":");
        Map<String, String> map = new HashMap<>();
        map.put(OAuth20.Keys.client_id.name(), split[0]);
        map.put(OAuth20.Keys.client_secret.name(), split[1]);

        Map<String, String> form = BasicHTTPServerFactory.formToMap(exchange);
        map.put(OAuth20.Keys.grant_type.name(), form.get(OAuth20.Keys.grant_type.name()));

        return map;
    }
}

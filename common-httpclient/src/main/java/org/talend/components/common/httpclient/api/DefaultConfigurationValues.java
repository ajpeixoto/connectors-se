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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Load default configuration value.
 *
 * Key are given with '.' as separator, but environment variable will be also retrieve. It is the same key with '.'
 * repalced by '_' and uppercase.
 * For instance "org.talend.a.key" will be "ORG_TALEND_A_KEY".
 */
public final class DefaultConfigurationValues {

    private DefaultConfigurationValues() {
        /* Don't instantiate. */
    }

    public final static String HTTP_CLIENT_CONNECT_TIMEOUT = "org.talend.http.client.connection.timeout";

    public final static String HTTP_CLIENT_RECEIVE_TIMEOUT = "org.talend.http.client.receive.timeout";

    /**
     * Duration to substract to expiresin of token to let time to do the call.
     */
    public final static String HTTP_CLIENT_EXPIRESIN_TOKEN_SECURITY_DURATION =
            "org.talend.http.client.token.expiresin.security.duration";

    public final static String HTTP_CLIENT_URL_PLACE_HOLDER_BEGIN = "org.talend.http.client.url.place.holder.begin";

    public final static String HTTP_CLIENT_URL_PLACE_HOLDER_END = "org.talend.http.client.url.place.holder.end";

    public final static String HTTP_CLIENT_ACCEPT_REDIRECTIONS =
            "org.talend.http.client.accept.redirection";

    public final static String HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI =
            "org.talend.http.client.max.number.redirections.on.same.host";

    public final static String HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS =
            "org.talend.http.client.accept.only.same.host.redirections";

    public final static String HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS =
            "org.talend.http.client.accept.relative.redirections";

    public final static String HTTP_CLIENT_OAUTH_TOKEN_FORCED_EXPIRES_IN =
            "org.talend.http.client.oauth.token.forced.expires_in";

    // Need to wait for https://issues.apache.org/jira/browse/CXF-8752
    /*
     * public static String HTTP_CLIENT_ALLOWED_REDIRECTED_VERBS =
     * "org.talend.http.client.redirections.allowed.verbs";
     */

    public static long HTTP_CLIENT_CONNECT_TIMEOUT_VALUE;

    public static long HTTP_CLIENT_RECEIVE_TIMEOUT_VALUE;

    public static long HTTP_CLIENT_EXPIRESIN_TOKEN_SECURITY_DURATION_VALUE;

    // Need to wait for https://issues.apache.org/jira/browse/CXF-8752
    // public static String HTTP_CLIENT_ALLOWED_REDIRECTED_VERBS_VALUE = getValue(HTTP_CLIENT_ALLOWED_REDIRECTED_VERBS);

    public static boolean HTTP_CLIENT_ACCEPT_REDIRECTIONS_VALUE;

    public static int HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI_VALUE;

    public static boolean HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS_VALUE;

    public static boolean HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS_VALUE;

    public static String HTTP_CLIENT_URL_PLACE_HOLDER_BEGIN_VALUE;

    public static String HTTP_CLIENT_URL_PLACE_HOLDER_END_VALUE;

    public static long HTTP_CLIENT_OAUTH_TOKEN_FORCED_EXPIRES_IN_VALUE;

    public final static int HTTP_CLIENT_CONNECT_TIMEOUT_DEFAULT_VALUE = 30000;

    public final static int HTTP_CLIENT_RECEIVE_TIMEOUT_DEFAULT_VALUE = 120000;

    public final static int HTTP_CLIENT_EXPIRESIN_TOKEN_SECURITY_DURATION_DEFAULT_VALUE = 5000;

    public final static boolean HTTP_CLIENT_ACCEPT_REDIRECTIONS_DEFAULT_VALUE = true;

    public final static int HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI_DEFAULT_VALUE = 3;

    public final static boolean HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS_DEFAULT_VALUE = false;

    public final static boolean HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS_DEFAULT_VALUE = true;

    public final static long HTTP_CLIENT_OAUTH_TOKEN_FORCED_EXPIRES_IN_DEFAULT_VALUE = Long.MIN_VALUE;

    public final static String HTTP_CLIENT_URL_PLACE_HOLDER_BEGIN_DEFAULT_VALUE = "{";

    public final static String HTTP_CLIENT_URL_PLACE_HOLDER_END_DEFAULT_VALUE = "}";

    static {
        reload();
    }

    public static void reload() {

        HTTP_CLIENT_CONNECT_TIMEOUT_VALUE =
                getValueAsLong(HTTP_CLIENT_CONNECT_TIMEOUT, HTTP_CLIENT_CONNECT_TIMEOUT_DEFAULT_VALUE);

        HTTP_CLIENT_RECEIVE_TIMEOUT_VALUE =
                getValueAsLong(HTTP_CLIENT_RECEIVE_TIMEOUT, HTTP_CLIENT_RECEIVE_TIMEOUT_DEFAULT_VALUE);

        HTTP_CLIENT_EXPIRESIN_TOKEN_SECURITY_DURATION_VALUE =
                getValueAsLong(HTTP_CLIENT_EXPIRESIN_TOKEN_SECURITY_DURATION,
                        HTTP_CLIENT_EXPIRESIN_TOKEN_SECURITY_DURATION_DEFAULT_VALUE);

        HTTP_CLIENT_ACCEPT_REDIRECTIONS_VALUE =
                getValueAsBoolean(HTTP_CLIENT_ACCEPT_REDIRECTIONS, HTTP_CLIENT_ACCEPT_REDIRECTIONS_DEFAULT_VALUE);
        HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI_VALUE =
                getValueAsInt(HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI,
                        HTTP_CLIENT_MAX_NUMBER_REDIRECTIONS_ON_SAME_URI_DEFAULT_VALUE);
        HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS_VALUE =
                getValueAsBoolean(HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS,
                        HTTP_CLIENT_ACCEPT_ONLY_SAME_HOST_REDIRECTIONS_DEFAULT_VALUE);
        HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS_VALUE =
                getValueAsBoolean(HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS,
                        HTTP_CLIENT_ACCEPT_RELATIVE_REDIRECTIONS_DEFAULT_VALUE);

        HTTP_CLIENT_URL_PLACE_HOLDER_BEGIN_VALUE =
                getValue(HTTP_CLIENT_URL_PLACE_HOLDER_BEGIN, HTTP_CLIENT_URL_PLACE_HOLDER_BEGIN_DEFAULT_VALUE);
        HTTP_CLIENT_URL_PLACE_HOLDER_END_VALUE =
                getValue(HTTP_CLIENT_URL_PLACE_HOLDER_END, HTTP_CLIENT_URL_PLACE_HOLDER_END_DEFAULT_VALUE);

        HTTP_CLIENT_OAUTH_TOKEN_FORCED_EXPIRES_IN_VALUE = getValueAsLong(HTTP_CLIENT_OAUTH_TOKEN_FORCED_EXPIRES_IN,
                HTTP_CLIENT_OAUTH_TOKEN_FORCED_EXPIRES_IN_DEFAULT_VALUE);
    }

    private static Map<String, String> varEnvNameCache;

    public static int getValueAsInt(String key, int def) {
        String val = getValue(key);
        return val == null ? def : Integer.parseInt(val);
    }

    public static long getValueAsLong(String key, long def) {
        String val = getValue(key);
        return val == null ? def : Long.parseLong(val);
    }

    public static boolean getValueAsBoolean(String key, boolean def) {
        String val = getValue(key);
        return val == null ? def : Boolean.parseBoolean(val);
    }

    public static String getValue(String key, String def) {
        String val = getValue(key);
        return Optional.ofNullable(val).orElse(def);
    }

    private static String getValue(String key) {
        String val = System.getenv(getVarEnvName(key));

        if (val == null) {
            val = System.getProperty(key);
        }

        return val;
    }

    /**
     * Transform a property key to environmental variable.
     * It is public only for unit tests purpose.
     * 
     * @param propertyName The property key.
     * @return The environmental variable name.
     */
    public static String getVarEnvName(final String propertyName) {
        if (varEnvNameCache == null) {
            varEnvNameCache = new HashMap<>();
        }
        return varEnvNameCache.computeIfAbsent(propertyName, k -> k.replaceAll("\\.", "_").toUpperCase());
    }

}

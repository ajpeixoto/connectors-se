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
package org.talend.components.common.httpclient.impl.cxf;

public class CXFConstants {

    public final static String AUTO_REDIRECT_SAME_HOST_ONLY = "http.redirect.same.host.only";

    public final static String AUTO_REDIRECT_ALLOW_REL_URI = "http.redirect.relative.uri";

    public final static String AUTO_REDIRECT_ALLOWED_URI = "http.redirect.allowed.uri";

    public final static String AUTO_REDIRECT_MAX_SAME_URI_COUNT = "http.redirect.max.same.uri.count";

    // TODO: Need to wait for https://issues.apache.org/jira/browse/CXF-8752
    // public final static String AUTHORIZED_REDIRECTED_HTTP_VERBS = "http.redirect.allowed.verbs";

    private CXFConstants() {
    }

}

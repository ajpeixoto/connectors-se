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
package org.talend.components.common.httpclient.factory;

import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.QueryConfiguration;
import org.talend.components.common.httpclient.impl.cxf.CXFHTTPClientImpl;

/**
 * In prevision of several implementations of HTTPClient.
 * Constructor of classes that implement HTTPClient should take a QueryConfiguration instance as parameter.
 */
public class HTTPClientFactory {

    private HTTPClientFactory() {
        /** Don't instantiate **/
    }

    public static HTTPClient create(QueryConfiguration queryConfiguration) {
        return new CXFHTTPClientImpl(queryConfiguration);
    }

}

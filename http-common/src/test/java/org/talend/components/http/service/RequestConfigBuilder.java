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
package org.talend.components.http.service;

import org.talend.components.common.httpclient.api.BodyFormat;
import org.talend.components.http.configuration.Datastore;
import org.talend.components.http.configuration.Dataset;
import org.talend.components.http.configuration.OutputContent;
import org.talend.components.http.configuration.RequestBody;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.configuration.auth.Authentication;
import org.talend.components.http.configuration.auth.Authorization;

import java.util.ArrayList;
import java.util.Collections;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestConfigBuilder {

    public static RequestConfig getEmptyRequestConfig() {
        RequestConfig config = new RequestConfig();

        Authentication authent = new Authentication();
        authent.setType(Authorization.AuthorizationType.NoAuth);

        Datastore datastore = new Datastore();
        datastore.setAuthentication(authent);
        datastore.setConnectionTimeout(5000);
        datastore.setReceiveTimeout(5000);

        RequestBody body = new RequestBody();
        body.setType(BodyFormat.TEXT);
        body.setTextValue("");

        Dataset dataset = new Dataset();
        dataset.setDatastore(datastore);
        dataset.setBody(body);
        dataset.setHasQueryParams(false);
        dataset.setQueryParams(new ArrayList<>());
        dataset.setHasHeaders(false);
        dataset.setHeaders(new ArrayList<>());
        dataset.setHasPathParams(false);
        dataset.setPathParams(new ArrayList<>());
        dataset.setReturnedContent(OutputContent.STATUS_HEADERS_BODY); // setCompletePayload(true);
        dataset.setOutputKeyValuePairs(false);

        config.setDataset(dataset);

        return config;
    }

    public static RequestConfig getEmptyProcessorRequestConfig() {
        RequestConfig config = new RequestConfig();

        Authentication authent = new Authentication();
        authent.setType(Authorization.AuthorizationType.NoAuth);

        Datastore datastore = new Datastore();
        datastore.setAuthentication(authent);
        datastore.setConnectionTimeout(5000);
        datastore.setReceiveTimeout(5000);

        RequestBody body = new RequestBody();
        body.setType(BodyFormat.TEXT);
        body.setTextValue("");

        Dataset dataset = new Dataset();
        dataset.setDatastore(datastore);
        dataset.setBody(body);
        dataset.setHasQueryParams(false);
        dataset.setQueryParams(Collections.emptyList());
        dataset.setHasHeaders(false);
        dataset.setHeaders(Collections.emptyList());
        dataset.setHasPathParams(false);
        dataset.setPathParams(Collections.emptyList());
        dataset.setReturnedContent(OutputContent.STATUS_HEADERS_BODY); // setCompletePayload(true);
        dataset.setOutputKeyValuePairs(false);

        config.setDataset(dataset);

        return config;
    }

}

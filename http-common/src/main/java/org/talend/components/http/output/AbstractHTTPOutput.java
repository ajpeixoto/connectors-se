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
package org.talend.components.http.output;

import org.talend.components.common.httpclient.api.HTTPClient;
import org.talend.components.common.httpclient.api.QueryConfiguration;
import org.talend.components.http.configuration.RequestConfig;
import org.talend.components.http.service.I18n;
import org.talend.components.http.service.httpClient.HTTPClientService;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.http.Response;

import java.io.Serializable;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.Getter;

public abstract class AbstractHTTPOutput<T> implements Serializable {

    @Getter(AccessLevel.PROTECTED)
    private final RequestConfig config;

    @Getter(AccessLevel.PROTECTED)
    private final HTTPClientService client;

    @Getter(AccessLevel.PROTECTED)
    private final I18n i18n;

    @Getter(AccessLevel.PROTECTED)
    private transient HTTPClient.HTTPResponse lastServerResponse;

    public AbstractHTTPOutput(final T config, final HTTPClientService client, final I18n i18n) {
        this.client = client;
        this.i18n = i18n;
        this.config = this.translateConfiguration(config);
    }

    @ElementListener
    public void process(final Record input) {
        try {
            Optional<QueryConfiguration> nextPageConfiguration = Optional.empty();

            do {
                QueryConfiguration queryConfiguration =
                        nextPageConfiguration.orElseGet(() -> client.convertConfiguration(config, input));
                lastServerResponse = client.invoke(queryConfiguration, config.isDieOnError());
                nextPageConfiguration = lastServerResponse.nextPageQueryConfiguration();
            } while (nextPageConfiguration.isPresent());

        } catch (Exception e) {
            ComponentException ce = new ComponentException(ComponentException.ErrorOrigin.BACKEND,
                    i18n.httpClientException(String.valueOf(e.getClass()), e.getMessage()));
            ce.setStackTrace(e.getStackTrace());
            throw ce;
        }
    }

    protected abstract RequestConfig translateConfiguration(T config);

}

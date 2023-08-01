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
package org.talend.components.http.service.provider;

import java.util.ServiceLoader;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.http.configuration.RequestConfig;

public interface JsonContentProvider {

    ContentFormat provideJsonContentFormat(RequestConfig config);

    static JsonContentProvider getProvider() {
        ServiceLoader<JsonContentProvider> serviceLoader = ServiceLoader.load(JsonContentProvider.class);

        if (serviceLoader.iterator().hasNext()) {
            return serviceLoader.iterator().next();
        } else {
            return config -> {
                JsonConfiguration jsonConfiguration = new JsonConfiguration();
                jsonConfiguration.setJsonPointer(config.getDataset().getSelector());
                jsonConfiguration.setForceDouble(config.getDataset().isJsonForceDouble());
                jsonConfiguration.setPathTypeList(config.getDataset().getPathTypeList());

                return jsonConfiguration;
            };
        }
    }
}

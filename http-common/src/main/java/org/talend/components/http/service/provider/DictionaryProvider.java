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
import java.util.function.UnaryOperator;

import javax.json.stream.JsonParserFactory;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public interface DictionaryProvider {

    UnaryOperator<String> createDictionary(Record rec, JsonParserFactory jsonParserFactory,
            RecordBuilderFactory recordBuilderFactory, boolean flag);

    static DictionaryProvider getProvider() {
        ServiceLoader<DictionaryProvider> serviceLoader = ServiceLoader.load(DictionaryProvider.class);

        if (serviceLoader.iterator().hasNext()) {
            return serviceLoader.iterator().next();
        } else {
            return (rec, jsonParserFactory, recordBuilderFactory, logIfNotFound) -> s -> {
                throw new UnsupportedOperationException("DSSL is not supported in the SE code.");
            };
        }
    }
}

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
package org.talend.components.rejector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.rejector.component.source.RejectorInputConfiguration;
import org.talend.components.rejector.configuration.RejectorDataSet;
import org.talend.components.rejector.configuration.RejectorDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Input;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.input.PartitionMapperImpl;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WithComponents("org.talend.components.rejector")
public class RejectorTest {

    @Injected
    private ComponentsHandler handler;

    @Service
    private LocalConfiguration configuration;

    @Test
    @DisplayName("Infinite stream mapper")
    void missingDriverConfig() {
        RejectorDataStore store = new RejectorDataStore();
        store.setUrl("http://url.com");
        RejectorDataSet dataSet = new RejectorDataSet();
        dataSet.setDataStore(store);
        dataSet.setAnInteger(1000);
        dataSet.setInformations("infos");
        RejectorInputConfiguration config = new RejectorInputConfiguration();
        config.setDataSet(dataSet);
        config.setRecordsNumber(40);
        String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        configURI += "&configuration.$maxDurationMs=2000&configuration.$maxRecords=5";
        Job
                .components()
                .component("rejector", "Rejector://InfiniteSource?" + configURI)
                .component("collector", "test://collector")
                .connections()
                .from("rejector")
                .to("collector")
                .build()
                .run();
        final List<Record> data = new ArrayList<>(handler.getCollectedData(Record.class));
        assertEquals(5, data.size());
    }

    @Test
    void testConfig() {
        Mapper mapper = handler.asManager()
                .findMapper("Rejector", "InfiniteStoppableSource", 1, new HashMap<String, String>() {

                    {
                        put("configuration.dataSet.dataStore.anInteger", "200");
                        put("configuration.$maxDurationMs", "200");
                        put("configuration.$maxRecords", "5");
                    }
                })
                .get();
        Map<String, String> internals = PartitionMapperImpl.class.cast(mapper).getInternalConfiguration();
        log.warn("[testConfig] internals: {}", internals);
        Input input = null;
        mapper.start();
        input = mapper.create();
        input.start();
        log.warn("[testConfig] {}", input.next());
        input.stop();
        mapper.stop();
    }

}

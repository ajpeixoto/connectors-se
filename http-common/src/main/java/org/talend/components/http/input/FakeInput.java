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
package org.talend.components.http.input;

import org.talend.components.http.configuration.RequestConfig;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;

import java.io.Serializable;

/**
 * This is a connector created to be sure the tck plugin is loaded.
 * This connector should not be used nor visible in applications.
 */
@Version(1)
@Icon(value = Icon.IconType.STAR)
@Emitter(name = "fake")
@Documentation("Fake input connector in test, to let component manager load 'org.talend.components.http.common' tck plugin since no real defined connector.")
public class FakeInput implements Serializable {

    public FakeInput(@Option RequestConfig configuration) {
        // Do nothing as fake connector
    }

    @Producer
    public Record next() {
        return null;
    }

}

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
package org.talend.components.http.configuration.pagination;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "location" }),
        @GridLayout.Row({ "offsetParamName", "offsetValue" }),
        @GridLayout.Row({ "limitParamName", "limitValue" }),
        @GridLayout.Row({ "elementsPath" })
})
@Documentation("Offset/max HTTP pagination strategy configuration.")
public class OffsetLimitStrategyConfig implements Serializable {

    @Option
    @Documentation("Where to set the pagination offset/max configuration.")
    @DefaultValue("QUERY_PARAMETERS")
    private Location location = Location.QUERY_PARAMETERS;

    @Option
    @Documentation("Name of the offset parameter.")
    private String offsetParamName;

    @Option
    @Documentation("Position of the first element to be retrieved for the first page.")
    private String offsetValue;

    @Option
    @Documentation("Name of the max parameter.")
    private String limitParamName;

    @Option
    @Documentation("Number of elements to retrieve per page.")
    private String limitValue;

    @Option
    @Documentation("Path to the list of element contained by the page.")
    private String elementsPath;

    public enum Location {
        QUERY_PARAMETERS,
        HEADERS
    }
}

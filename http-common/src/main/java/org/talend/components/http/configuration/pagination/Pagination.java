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
import org.talend.components.http.service.UIService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "preset" }),
        @GridLayout.Row({ "strategy" }),
        @GridLayout.Row({ "offsetLimitStrategyConfig" })
})
@Documentation("HTTP pagination configuration.")
public class Pagination implements Serializable {

    @Option
    @Documentation("Preconfigured pagination flavor.")
    @Suggestable(value = UIService.ACTION_PAGINATION_FLAVOR_LIST)
    private String preset;

    @Option
    @Documentation("Where to set the pagination configuration.")
    @DefaultValue("OFFSET_LIMIT")
    private Strategy strategy = Strategy.OFFSET_LIMIT;

    @Option
    @Documentation("Offset/max pagination strategy configuration.")
    @ActiveIf(target = "strategy", value = "OFFSET_LIMIT")
    private OffsetLimitStrategyConfig offsetLimitStrategyConfig;

    public enum Strategy {
        OFFSET_LIMIT
    }

}

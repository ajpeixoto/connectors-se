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
package org.talend.components.bigquery.dataset;

import lombok.Data;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery-connector")
@DataSet("BigQueryDataSetTableType")
@Documentation("Dataset of a BigQuery component for table type.")
@GridLayout({ @GridLayout.Row("connection"), @GridLayout.Row("bqDataset"), @GridLayout.Row("tableName"),
        @GridLayout.Row("gsBucket") })
public class TableDataSet implements Serializable {

    @Option
    @Documentation("The BigQuery datastore")
    private BigQueryConnection connection;

    @Option
    @Suggestable(value = BigQueryService.ACTION_SUGGESTION_DATASET, parameters = { "connection" })
    @Documentation("The BigQuery dataset")
    private String bqDataset;

    @Option
    @Suggestable(value = BigQueryService.ACTION_SUGGESTION_TABLES, parameters = { "connection", "bqDataset" })
    @Documentation("The BigQuery table name")
    private String tableName;

    @Option
    @Documentation("Google Storage bucket for temporary files")
    private String gsBucket;

}
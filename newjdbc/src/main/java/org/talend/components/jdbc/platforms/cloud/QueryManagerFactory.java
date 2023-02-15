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
package org.talend.components.jdbc.platforms.cloud;

import lombok.Data;
import org.talend.components.jdbc.output.JDBCOutputConfig;
import org.talend.components.jdbc.platforms.Platform;
import org.talend.components.jdbc.service.I18nMessage;

import static java.util.Locale.ROOT;
import static org.talend.components.jdbc.platforms.SnowflakePlatform.SNOWFLAKE;

@Data
public final class QueryManagerFactory {

    private QueryManagerFactory() {
    }

    public static QueryManagerImpl getQueryManager(final Platform platform, final I18nMessage i18n,
            final JDBCOutputConfig configuration) {
        final String db = configuration.getDataSet().getDataStore().getDbType().toLowerCase(ROOT);
        switch (db) {
        case SNOWFLAKE:
            switch (configuration.getDataAction()) {
            case INSERT:
                return new SnowflakeInsert(platform, configuration, i18n);
            case UPDATE:
                return new SnowflakeUpdate(platform, configuration, i18n);
            case DELETE:
                return new SnowflakeDelete(platform, configuration, i18n);
            case INSERT_OR_UPDATE:
                return new SnowflakeUpsert(platform, configuration, i18n);
            default:
                throw new IllegalStateException(i18n.errorUnsupportedDatabaseAction());
            }
        default:
            return null;// other database type use generic studio way
        }
    }

}

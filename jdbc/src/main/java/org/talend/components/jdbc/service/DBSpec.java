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
package org.talend.components.jdbc.service;

import java.util.regex.Pattern;

public class DBSpec {

    // see https://docs.snowflake.com/en/sql-reference/functions-table
    private static Pattern SNOWFLAKE_TABLE_FUNCTION_PATTERN =
            Pattern.compile("^SELECT\\s+[^;]+\\bFROM\\b([^;]*)\\bTABLE\\s*\\([^;]+\\)([^;]*);?$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);

    public static boolean isSnowflakeTableFunction(final String query, final String dbType) {
        if (query == null) {
            return false;
        }

        if ("Snowflake".equals(dbType) && SNOWFLAKE_TABLE_FUNCTION_PATTERN.matcher(query.trim()).matches()) {
            return true;
        }

        return false;
    }

}

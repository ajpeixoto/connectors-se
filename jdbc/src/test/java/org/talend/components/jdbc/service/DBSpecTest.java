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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.jdbc")
class DBSpecTest {

    @Test
    void isSnowflakeTableFunction() {
        Assertions.assertFalse(DBSpec.isSnowflakeTableFunction("", "Mysql"));
        Assertions.assertFalse(DBSpec.isSnowflakeTableFunction("select id, name from table('summary')", "Mysql"));
        Assertions.assertFalse(DBSpec.isSnowflakeTableFunction(null, "Snowflake"));
        Assertions.assertFalse(DBSpec.isSnowflakeTableFunction("select * from test", "Snowflake"));

        Assertions.assertTrue(DBSpec.isSnowflakeTableFunction("select id, name from table('summary')", "Snowflake"));
        Assertions.assertTrue(
                DBSpec.isSnowflakeTableFunction("select * from table(udf_function('2010','EUR'))", "Snowflake"));
        Assertions.assertTrue(
                DBSpec.isSnowflakeTableFunction("SELECT seq8() FROM table(generator(rowCount => 5))", "Snowflake"));
        Assertions.assertTrue(DBSpec.isSnowflakeTableFunction("SELECT city_name, temperature\n" +
                "    FROM TABLE(record_high_temperatures_for_date('2021-06-27'::DATE))\n" +
                "    ORDER BY city_name;", "Snowflake"));
        Assertions.assertTrue(DBSpec.isSnowflakeTableFunction("SELECT\n" +
                "        doi.event_date as \"Date\", \n" +
                "        record_temperatures.city,\n" +
                "        record_temperatures.temperature\n" +
                "    FROM dates_of_interest AS doi,\n" +
                "         TABLE(record_high_temperatures_for_date(doi.event_date)) AS record_temperatures\n" +
                "      ORDER BY doi.event_date, city; ", "Snowflake"));
    }
}

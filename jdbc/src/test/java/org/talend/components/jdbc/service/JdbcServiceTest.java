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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.*;

@WithComponents("org.talend.components.jdbc")
class JdbcServiceTest {

    @Service
    private JdbcService service;

    @ParameterizedTest
    @CsvSource(value = {
            "select * from MYTABLE|MySQL|false",
            "select * from MYTABLE;Select name from anotherTable|MySQL|true",
            "SELECT date_format(date_sub(current_TIMESTAMP(),interval 30 DAY),'%Y-%m-%dT00:00:00.000Z') AS Mon_Champ|MYSQL|false",
            "SELECT date_format(date_sub(current_TIMESTAMP(),interval 30 DAY),'%Y-%m-%dT00:00:00.000Z') AS Mon_Champ; select name from anotherTable|MYSQL|true",
            "insert into myTable (id) values ('1')|MYSQL|true",
            "update myTable set id=1 where id < 1|MYSQL|true"
    }, delimiter = '|')
    public void isInvalidSQLQueryTest(String query, String type, boolean result) {
        System.out.println("SQL : " + query);
        boolean invalidSQLQuery = service.isInvalidSQLQuery(query, type);
        Assertions.assertEquals(result, invalidSQLQuery);
    }

}
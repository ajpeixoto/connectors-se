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
package org.talend.components.jdbc.suite;

import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.output.OutputUtils;

import java.util.*;
import java.util.stream.Collectors;

class SimpleObj {

    private String name;

    private Map<String, String> map;

    public SimpleObj(String name, Map<String, String> map) {
        this.name = name;
        this.map = map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }
}

class OutputUtilsTest {

    @Test
    @DisplayName("should keep both simple object because distinct done on whole object")
    void getAllSchemaEntriesDistinct() {
        SimpleObj obj1 = new SimpleObj("id", new HashMap<String, String>());
        SimpleObj obj2 = new SimpleObj("id", new HashMap<String, String>() {

            {
                put("a", "b");
                put("c", "d");
            }
        });
        SimpleObj obj3 = new SimpleObj("name", new HashMap<String, String>());

        List<SimpleObj> list = Arrays.asList(obj1, obj2, obj3);
        List<SimpleObj> res = list.stream().distinct().collect(Collectors.toList());
        Assert.assertEquals(3, res.size());
    }

    @Test
    @DisplayName("should only keep the first simple object with name 'id' using distinct on attribute name")
    void getAllSchemaEntriesDistinctByKey() {
        SimpleObj obj1 = new SimpleObj("id", new HashMap<String, String>());
        SimpleObj obj2 = new SimpleObj("id", new HashMap<String, String>() {

            {
                put("a", "b");
                put("c", "d");
            }
        });
        SimpleObj obj3 = new SimpleObj("name", new HashMap<String, String>());

        List<SimpleObj> list = Arrays.asList(obj1, obj2, obj3);
        List<SimpleObj> res =
                list.stream().filter(OutputUtils.distinctByKey(SimpleObj::getName)).collect(Collectors.toList());
        Assert.assertEquals(2, res.size());
    }
}

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
package org.talend.components.common.httpclient.api;

import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultConfigurationValuesTest {

    @Test
    public void getDefaultValuesTests() {
        String key = "aa.bb.cc";
        int valueAsInt = DefaultConfigurationValues.getValueAsInt(key, 200);
        Assertions.assertEquals(200, valueAsInt);

        System.setProperty(key, "350");
        valueAsInt = DefaultConfigurationValues.getValueAsInt(key, 200);
        Assertions.assertEquals(350, valueAsInt);

        // Can't test retrieving value from System.getEnv() since it is an unmodifiable map.
    }

    public void getVarEnvNameTest() {
        String key = "aa.bb.cc-ddd_eeee.fffff";
        String varEnvName = DefaultConfigurationValues.getVarEnvName(key);
        Assertions.assertEquals("AA_BB_CC-DDD_EEEE_FFFFF", varEnvName);
    }

}
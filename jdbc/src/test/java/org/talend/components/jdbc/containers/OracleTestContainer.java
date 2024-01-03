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
package org.talend.components.jdbc.containers;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.OracleContainer;

import lombok.experimental.Delegate;

public class OracleTestContainer implements JdbcTestContainer {

    /**
     * image defined in test/resources/testcontainers.properties
     * wnameless/oracle-xe-11g@sha256:825ba799432809fc7200bb1d7ef954973a8991d7702a860c87177fe05301f7da
     */
    @Delegate(types = { DelegatedMembers.class })
    private final JdbcDatabaseContainer container = new OracleContainer();

    @Override
    public String getDatabaseType() {
        return "Oracle";
    }

    @Override
    public void close() {
        this.container.close();
    }
}

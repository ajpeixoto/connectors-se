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

public interface JdbcTestContainer extends AutoCloseable {

    /**
     * @return Database as defined in the configuration file with en property <code>jdbc.drivers[].id</code>
     */
    String getDatabaseType();

    String getUsername();

    String getPassword();

    String getJdbcUrl();

    default String getDriverClassName() {
        return "";
    }

    void start();

    void stop();

    @Override
    default void close() throws Exception {
        stop();
    }

    boolean isRunning();

    /**
     * Just to make a Delegate on containers with narrow scope
     */
    interface DelegatedMembers {

        String getUsername();

        String getPassword();

        String getJdbcUrl();

        String getDriverClassName();

        void start();

        void stop();

        boolean isRunning();
    }
}

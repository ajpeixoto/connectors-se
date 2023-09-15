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
package org.talend.components.http.service;

import java.util.concurrent.Callable;

import lombok.SneakyThrows;

public class ClassLoaderInvokeUtils {

    @SneakyThrows
    public static <T> T invokeInLoader(final Callable<T> callable, final ClassLoader loader) {
        final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(loader);
        try {
            return callable.call();
        } finally {
            Thread.currentThread().setContextClassLoader(oldLoader);
        }
    }

}

/*
 * Copyright (C) 2006-2022 Talend Inc. - www.talend.com
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
package org.talend.components.common.text;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

public class Substitutor {

    private static final char ESCAPE = '\\';

    private static final String DEFAULT_SEPARATOR = ":-";

    /**
     * given place holder (dictionnary)
     */
    private final UnaryOperator<String> placeholderProvider;

    /**
     * key finder with defined prefix / suffix
     */
    private final Substitutor.KeyFinder finder;

    /**
     * Constructor
     *
     * @param finder : finder;
     * @param placeholderProvider Function used to replace the string
     */
    public Substitutor(Substitutor.KeyFinder finder, UnaryOperator<String> placeholderProvider) {
        this.finder = finder;
        if (placeholderProvider instanceof Substitutor.CachedPlaceHolder) {
            this.placeholderProvider = placeholderProvider;
        } else {
            this.placeholderProvider = new Substitutor.CachedPlaceHolder(placeholderProvider);
        }
    }

    public String replace(final String source) {
        if (source == null) {
            return source;
        }

        if (source.trim().isEmpty()) {
            return source;
        }

        int prefixLength = this.finder.getPrefixWithKeyPrefix().length();
        int suffixLength = this.finder.getSuffix().length();

        if (source.length() < prefixLength + suffixLength) {
            return source;
        }

        StringBuilder output = new StringBuilder();

        int cursor = 0;

        boolean foundKey = false;
        int indSuffix = 0;
        do {
            foundKey = false;

            // Found given prefix from current position in source (cursor)
            int indPrefix = source.indexOf(this.finder.getPrefixWithKeyPrefix(), cursor);

            // If no new prefix found, concatenate until the end of source
            if (indPrefix < 0) {
                output.append(source.substring(cursor, source.length()));
                continue;
            }

            // Is the found prefix escaped ?
            boolean escaped = false;
            if (indPrefix > 0) {
                char previous = source.charAt(indPrefix - 1);
                escaped = previous == ESCAPE;
            }

            // If escaped, skip the escape char \ add the prefix and continue
            if (escaped) {
                output.append(source.substring(cursor, indPrefix - 1))
                        .append(this.finder.getPrefixWithKeyPrefix());
                cursor = indPrefix + prefixLength;
                foundKey = true;
                continue;
            }

            int indIntermediatePrefix = indPrefix;
            boolean hasPrefixIntermediate = false;

            // Search for suffix
            // If there are intermediate prefix/suffix skip them
            do {
                hasPrefixIntermediate = false;
                indSuffix = source.indexOf(this.finder.getSuffix(), indIntermediatePrefix);
                if (indSuffix < 0) {
                    continue;
                }

                // Is there another prefix between 1st prefix and suffix ?
                // Needed for such case: "xxx {.aaa.zzz{attr < 10}} yyy"
                indIntermediatePrefix = source.indexOf(this.finder.getPrefix(), indIntermediatePrefix + 1);
                hasPrefixIntermediate = indIntermediatePrefix >= 0 && indIntermediatePrefix < indSuffix;
                if (hasPrefixIntermediate) {
                    indIntermediatePrefix = indSuffix + 1;
                }
            } while (hasPrefixIntermediate);

            // Concatenate from cursor until found prefix
            output.append(source.substring(cursor, indPrefix));

            // Extract the key to replace
            String key = source.substring(indPrefix + prefixLength, indSuffix);
            foundKey = true;

            // Get the value or the given default
            output.append(getValue(key));

            // Move the position
            cursor = indSuffix + suffixLength;

        } while (foundKey);

        return output.toString();
    }

    private String getValue(String key) {
        String[] split = key.split(DEFAULT_SEPARATOR);
        String value = this.placeholderProvider.apply(split[0]);

        if (value == null && split.length > 1) {
            return split[1];
        }
        return value;
    }

    public static class KeyFinder {

        private final String prefix;

        private final String suffix;

        private final String keyPrefix;

        private final String prefixWithKeyPrefix;

        public KeyFinder(String prefix, String suffix) {
            this(prefix, suffix, null);
        }

        /**
         * The keyprefix can be usefull to distinguish several dictionaries.
         * For instance {.input.user.name} and {.response.user.name} in HTTPClient connector.
         *
         * @param prefix The placeholder prefix.
         * @param suffix The placeholder suffix.
         * @param keyPrefix If the extracted key start by this key it will be removed.
         */
        public KeyFinder(String prefix, String suffix, String keyPrefix) {
            this.prefix = prefix;
            this.suffix = suffix;
            this.keyPrefix = keyPrefix;
            this.prefixWithKeyPrefix = prefix + Optional.ofNullable(keyPrefix).orElse("");
        }

        public String getPrefix() {
            return this.prefix;
        }

        public String getPrefixWithKeyPrefix() {
            return this.prefixWithKeyPrefix;
        }

        public String getSuffix() {
            return this.suffix;
        }

        public String getKeyPrefix() {
            return this.keyPrefix;
        }

    }

    /**
     * To optimized research of key.
     */
    static class CachedPlaceHolder implements UnaryOperator<String> {

        /**
         * original place holder function.
         */
        private final UnaryOperator<String> originalFunction;

        /**
         * cache for function
         */
        private final Map<String, Optional<String>> cache = new HashMap<>();

        public CachedPlaceHolder(UnaryOperator<String> originalFunction) {
            super();
            this.originalFunction = originalFunction;
        }

        @Override
        public String apply(String varName) {
            return cache.computeIfAbsent(varName, k -> Optional.ofNullable(originalFunction.apply(k))).orElse(null);
        }
    }

}

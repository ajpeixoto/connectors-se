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
package org.talend.components.common.httpclient.pagination.location;

import org.talend.components.common.httpclient.api.KeyValuePair;
import org.talend.components.common.httpclient.api.pagination.OffsetLimitPagination;

import java.util.List;
import java.util.Optional;

public class PaginationLocationService {

    public List<KeyValuePair> initKeyValuePairs(final List<KeyValuePair> kvps,
            final OffsetLimitPagination offsetLimitPagination) {
        kvps.add(new KeyValuePair(offsetLimitPagination.getOffsetParamName(), offsetLimitPagination.getOffsetValue()));
        kvps.add(new KeyValuePair(offsetLimitPagination.getLimitParamName(), offsetLimitPagination.getLimitValue()));

        return kvps;
    }

    public List<KeyValuePair> updateListKeyValuePair(final List<KeyValuePair> kvps,
            final OffsetLimitPagination offsetLimitPagination,
            final int nbReceivedElements) {
        String offsetParamName = offsetLimitPagination.getOffsetParamName();
        String limitParamName = offsetLimitPagination.getLimitParamName();

        Optional<KeyValuePair> existingOffset =
                kvps.stream().filter(h -> h.getKey().equals(offsetParamName)).findFirst();

        if (existingOffset.isPresent()) {
            existingOffset.get()
                    .setValue(nextOffset(existingOffset.get().getValue(), nbReceivedElements));
        } else {
            kvps.add(new KeyValuePair(offsetParamName, offsetLimitPagination.getOffsetValue()));
        }

        Optional<KeyValuePair> existingLimit = kvps.stream().filter(h -> h.getKey().equals(limitParamName)).findFirst();
        if (!existingLimit.isPresent()) {
            kvps.add(new KeyValuePair(limitParamName, offsetLimitPagination.getLimitValue()));
        }

        return kvps;
    }

    public String nextOffset(String previousOffset, int lastNbElements) {
        long previousOffsetLong = Long.parseLong(previousOffset);

        return String.valueOf(previousOffsetLong + lastNbElements);
    }

}

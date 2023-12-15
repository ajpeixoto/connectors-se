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

import org.talend.components.common.httpclient.api.QueryConfiguration;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.stream.JsonParser;
import java.io.StringReader;
import java.util.*;
import java.util.stream.Collectors;

public class PaginationLocationBody implements PaginationLocation {

    private final PaginationLocationService paginationLocationService;

    public PaginationLocationBody() {
        this.paginationLocationService = new PaginationLocationService();
    }

    @Override
    public void setPagination(QueryConfiguration queryConfiguration) {
        String limitPath = queryConfiguration.getOffsetLimitPagination().getLimitParamName();
        String limitValue = queryConfiguration.getOffsetLimitPagination().getLimitValue();
        String offsetPath = queryConfiguration.getOffsetLimitPagination().getOffsetParamName();
        String offsetValue = queryConfiguration.getOffsetLimitPagination().getOffsetValue();
        setPaginationToBody(queryConfiguration, limitPath, limitValue);
        setPaginationToBody(queryConfiguration, offsetPath, offsetValue);
    }

    @Override
    public void updatePagination(QueryConfiguration queryConfiguration, int nbReceived) {
        String limitPath = queryConfiguration.getOffsetLimitPagination().getLimitParamName();
        String limitValue = queryConfiguration.getOffsetLimitPagination().getLimitValue();
        String offsetPath = queryConfiguration.getOffsetLimitPagination().getOffsetParamName();
        setPaginationToBody(queryConfiguration, limitPath, limitValue);
        String offsetValue = getOffsetValue(queryConfiguration, offsetPath);
        setPaginationToBody(queryConfiguration, offsetPath,
                paginationLocationService.nextOffset(offsetValue, nbReceived));
    }

    private String getOffsetValue(QueryConfiguration queryConfiguration, String path) {
        // Remove trailing '.' characters.
        while (path.startsWith(".")) {
            path = path.substring(1);
        }

        while (path.endsWith(".")) {
            path = path.substring(0, path.length() - 1);
        }

        List<String> existedPaths = new ArrayList<>();
        // Retrieve all segment
        List<String> paths = Arrays.stream(path.split("\\.")).collect(Collectors.toList());
        JsonReader jsonReader = Json.createReader(new StringReader(queryConfiguration.getPlainTextBody()));
        JsonObject currentObject = jsonReader.readObject();
        String offsetValue = "0";
        if (paths.size() > 0) {
            for (int i = 0; i < paths.size(); i++) {
                boolean last = i == (paths.size() - 1);

                if (last) {
                    offsetValue = currentObject.getString(paths.get(i));
                } else {
                    currentObject = currentObject.getJsonObject(paths.get(i));
                }

            }
        }
        jsonReader.close();
        return offsetValue;
    }

    private List<String> findListExistedPaths(QueryConfiguration queryConfiguration, String path) {
        // Remove trailing '.' characters.
        while (path.startsWith(".")) {
            path = path.substring(1);
        }

        while (path.endsWith(".")) {
            path = path.substring(0, path.length() - 1);
        }

        List<String> existedPaths = new ArrayList<>();
        // Retrieve all segment
        List<String> paths = Arrays.stream(path.split("\\.")).collect(Collectors.toList());
        JsonReader jsonReader = Json.createReader(new StringReader(queryConfiguration.getPlainTextBody()));
        JsonObject currentObject = jsonReader.readObject();
        if (paths.size() > 1) {
            for (int i = 0; i < paths.size() - 1; i++) {
                if (currentObject == null) {
                    break;
                }
                currentObject = currentObject.getJsonObject(paths.get(i));
                existedPaths.add(paths.get(i));

            }
        }
        return existedPaths;
    }

    private void setPaginationToBody(QueryConfiguration queryConfiguration, String path, String value) {
        // Remove trailing '.' characters.
        while (path.startsWith(".")) {
            path = path.substring(1);
        }

        while (path.endsWith(".")) {
            path = path.substring(0, path.length() - 1);
        }

        // Retrieve all segment
        List<String> paths = Arrays.stream(path.split("\\.")).collect(Collectors.toList());
        Collections.reverse(paths);
        if (queryConfiguration.getPlainTextBody() != null) {
            JsonParser parser = Json.createParser(new StringReader(queryConfiguration.getPlainTextBody()));
            JsonObjectBuilder requestBuilder = Json.createObjectBuilder();
            JsonObject sourceJsonObject = parser.getObject().asJsonObject();
            sourceJsonObject.forEach(requestBuilder::add);

            JsonObject currentObject = null;
            if (paths.size() > 1) {
                for (int i = 0; i < paths.size() - 1; i++) {
                    if (currentObject == null) {
                        currentObject = Json.createObjectBuilder().add(paths.get(i), value).build();
                    } else {
                        currentObject = Json.createObjectBuilder().add(paths.get(i), currentObject).build();
                    }
                }
                requestBuilder.add(paths.get(paths.size() - 1), currentObject);
            } else {
                requestBuilder.add(paths.get(0), Json.createValue(value));
            }
            queryConfiguration.setPlainTextBody(requestBuilder.build().toString());

            parser.close();
        }
    }
}

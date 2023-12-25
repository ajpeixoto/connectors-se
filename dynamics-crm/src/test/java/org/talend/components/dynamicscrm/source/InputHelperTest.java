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
package org.talend.components.dynamicscrm.source;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.components.dynamicscrm.source.DynamicsCrmInputMapperConfiguration.Operator;
import org.talend.components.dynamicscrm.source.FilterCondition.FilterOperator;
import org.talend.components.dynamicscrm.source.OrderByCondition.Order;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@WithComponents("org.talend.components.dynamicscrm")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InputHelperTest {

    @Service
    private I18n i18n;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private InputHelper helper;

    @BeforeAll
    public void initHelper() {
        helper = new InputHelper(i18n);
    }

    @Test
    void testEmptyFilterConversion() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNull(filterQuery);
    }

    @Test
    void testOneFieldEquals() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.EQUAL, "value"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field eq 'value')", filterQuery);
    }

    @Test
    void testOneFieldNotEquals() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.NOTEQUAL, "value"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field ne 'value')", filterQuery);
    }

    @Test
    void testOneFieldGreater() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_THAN, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field gt '5')", filterQuery);
    }

    @Test
    void testOneFieldGreaterOrEqual() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field ge '5')", filterQuery);
    }

    @Test
    void testOneFieldLess() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_THAN, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field lt '5')", filterQuery);
    }

    @Test
    void testOneFieldLessOrEqual() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field le '5')", filterQuery);
    }

    @Test
    void testMultipleAnd() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.EQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.NOTEQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_OR_EQUAL, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals(
                "(field eq 'value') and (field ne 'value') and (field gt '5') and (field ge '5') and (field lt '5') and (field le '5')",
                filterQuery);
    }

    @Test
    void testMultipleOr() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        configuration.setOperator(Operator.OR);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.EQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.NOTEQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_OR_EQUAL, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals(
                "(field eq 'value') or (field ne 'value') or (field gt '5') or (field ge '5') or (field lt '5') or (field le '5')",
                filterQuery);
    }

    @Test
    void testFilterIntField() {
        String intColumnName = "intColumn";
        Schema schema = recordBuilderFactory.newRecordBuilder().withInt(intColumnName, 1).build().getSchema();

        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        configuration.setOperator(Operator.OR);
        List<FilterCondition> filterConditionList = Collections.singletonList(
                new FilterCondition(intColumnName, FilterOperator.EQUAL, "1"));

        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(schema, configuration);

        Assertions.assertFalse(filterQuery.contains("'"));
        Assertions.assertTrue(filterQuery.contains(intColumnName));
        Assertions.assertTrue(filterQuery.contains("eq"));
        Assertions.assertTrue(filterQuery.contains("1"));
    }

    @Test
    void testFilterSeveralNoStringFields() {
        String longColumnName = "longColumn";
        String floatColumnName = "floatColumn";
        String doubleColumnName = "doubleColumn";
        String booleanColumnName = "booleanColumn";
        String dateTimeColumnName = "dateTimeColumn";
        String decimalColumnName = "decimalColumn";

        Instant dateTimeValue = Instant.now();
        Schema schema = recordBuilderFactory.newRecordBuilder()
                .withLong(longColumnName, 1L)
                .withFloat(floatColumnName, 1.0f)
                .withDouble(doubleColumnName, 1.0)
                .withBoolean(booleanColumnName, true)
                .withDateTime(dateTimeColumnName, dateTimeValue.atZone(ZoneId.systemDefault()))
                .withDecimal(decimalColumnName, new BigDecimal("1234567890"))
                .build()
                .getSchema();

        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        configuration.setOperator(Operator.OR);
        List<FilterCondition> filterConditionList = new ArrayList<>();

        filterConditionList.add(new FilterCondition(longColumnName, FilterOperator.EQUAL, "1"));
        filterConditionList.add(new FilterCondition(floatColumnName, FilterOperator.EQUAL, "1.0"));
        filterConditionList.add(new FilterCondition(doubleColumnName, FilterOperator.EQUAL, "1.0"));
        filterConditionList.add(new FilterCondition(booleanColumnName, FilterOperator.EQUAL, "true"));
        filterConditionList.add(new FilterCondition(
                dateTimeColumnName, FilterOperator.NOTEQUAL, Instant.now().toString()));
        filterConditionList.add(new FilterCondition(decimalColumnName, FilterOperator.EQUAL, "1234567890"));

        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(schema, configuration);

        Assertions.assertFalse(filterQuery.contains("'"));
        assertEquals(filterConditionList.size(), filterQuery.split("eq").length);
    }

    @Test
    void testFilterOneStringOneNotStringColumns() {
        String longColumnName = "longColumn";
        String stringColumnName = "stringColumn";

        Schema schema = recordBuilderFactory.newRecordBuilder()
                .withLong(longColumnName, 1L)
                .withString(stringColumnName, "abc")
                .build()
                .getSchema();

        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        configuration.setOperator(Operator.OR);
        List<FilterCondition> filterConditionList = new ArrayList<>();

        filterConditionList.add(new FilterCondition(longColumnName, FilterOperator.EQUAL, "1"));
        filterConditionList.add(new FilterCondition(stringColumnName, FilterOperator.EQUAL, "abc"));

        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(schema, configuration);

        Assertions.assertEquals(filterQuery.length() - 2,
                filterQuery.replaceAll("'", "").length());
    }

    @Test
    void testOrderByFieldAsc() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Arrays.asList(new OrderByCondition("field", Order.ASC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNotNull(orderQuery);
        assertEquals("field", orderQuery);
    }

    @Test
    void testOrderByFieldDesc() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Arrays.asList(new OrderByCondition("field", Order.DESC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNotNull(orderQuery);
        assertEquals("field desc", orderQuery);
    }

    @Test
    void testOrderBySeveralFields() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration
                .setOrderByConditionsList(
                        Arrays
                                .asList(new OrderByCondition("field", Order.DESC),
                                        new OrderByCondition("field1", Order.ASC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNotNull(orderQuery);
        assertEquals("field desc,field1", orderQuery);
    }

    @Test
    void testEmptyFieldOrderBy() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Arrays.asList(new OrderByCondition("", Order.DESC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNull(orderQuery);
    }

    @Test
    void testEmptyOrderBy() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Collections.emptyList());

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNull(orderQuery);
    }

    @Test
    void testNullListOrderBy() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(null);

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNull(orderQuery);
    }

}

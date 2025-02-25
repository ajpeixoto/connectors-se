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
package org.talend.components.common.stream.output.excel;

import java.sql.Date;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Supplier;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

/**
 * Transform record format to excel format.
 */
public class RecordToExcel {

    private static final short DATE_EXCEL_FORMAT = (short) 14;

    private CellStyle cachedDateCellStyle;

    /**
     * Build excel row from record.
     *
     * @param constructor : row builder.
     * @param rec : record for read values.
     * @return excel row.
     */
    public Row from(Supplier<Row> constructor, Record rec) {
        final Row row = constructor.get();
        final List<Entry> entries = rec.getSchema().getEntries();

        for (Schema.Entry entry : entries) {
            final Cell cell = this.addCell(row);
            this.majCellValue(cell, rec, entry);
        }
        return row;
    }

    /**
     * build excel header from record schema.
     *
     * @param constructor : row builder.
     * @param schema: input schema.
     * @return excel row with title.
     */
    public Row buildHeader(Supplier<Row> constructor, Schema schema) {
        final Row headerRow = constructor.get();
        schema.getEntries().forEach((Entry e) -> this.buildHeaderCell(headerRow, e));
        return headerRow;
    }

    /**
     * build cell from row & entry.
     *
     * @param headerRow : excel row for header.
     * @param entry : record schema entry.
     */
    private void buildHeaderCell(Row headerRow, Schema.Entry entry) {
        final Cell headerCell = addCell(headerRow);
        headerCell.setCellValue(entry.getName());
    }

    /**
     * Simply add a cell to a row.
     *
     * @param row : row it add a cell.
     * @return added cell.
     */
    private Cell addCell(Row row) {
        return row.createCell(row.getPhysicalNumberOfCells());
    }

    private void majCellValue(Cell cell, Record rec, Schema.Entry entry) {

        final String name = entry.getName();
        switch (entry.getType()) {
        case BOOLEAN:
            cell.setCellType(CellType.BOOLEAN);
            final Optional<Boolean> optionalBoolean = rec.getOptionalBoolean(name);
            if (optionalBoolean.isPresent()) {
                cell.setCellValue(optionalBoolean.get());
            }
            break;
        case DATETIME:
            cell.setCellType(CellType.NUMERIC);
            // use built-in format: m/d/yy, org.apache.poi.ss.usermodel.BuiltinFormats
            if (cachedDateCellStyle == null) {
                final CellStyle cellStyle = cell.getSheet().getWorkbook().createCellStyle();
                cellStyle.setDataFormat(DATE_EXCEL_FORMAT);
                cachedDateCellStyle = cellStyle;
            }
            cell.setCellStyle(cachedDateCellStyle);
            final Optional<ZonedDateTime> optionalDateTime = rec.getOptionalDateTime(name);
            if (optionalDateTime.isPresent()) {
                cell.setCellValue(Date.from(optionalDateTime.get().toInstant()));
            }
            break;
        case INT:
            cell.setCellType(CellType.NUMERIC);
            final OptionalInt optionalInt = rec.getOptionalInt(name);
            if (optionalInt.isPresent()) {
                cell.setCellValue(optionalInt.getAsInt());
            }
            break;
        case LONG:
            cell.setCellType(CellType.NUMERIC);
            final OptionalLong optionalLong = rec.getOptionalLong(name);
            if (optionalLong.isPresent()) {
                cell.setCellValue(optionalLong.getAsLong());
            }
            break;
        case FLOAT:
            cell.setCellType(CellType.NUMERIC);
            final OptionalDouble optionalFloat = rec.getOptionalFloat(name);
            if (optionalFloat.isPresent()) {
                cell.setCellValue(optionalFloat.getAsDouble());
            }
            break;
        case DOUBLE:
            cell.setCellType(CellType.NUMERIC);
            final OptionalDouble optionalDouble = rec.getOptionalDouble(name);
            if (optionalDouble.isPresent()) {
                cell.setCellValue(optionalDouble.getAsDouble());
            }
            break;
        case BYTES:
            cell.setCellType(CellType.STRING);
            cell.setCellValue(Arrays.toString(rec.getBytes(name)));
            break;
        default:
            cell.setCellType(CellType.STRING);
            cell.setCellValue(String.valueOf(rec.get(Object.class, entry.getName())));
        }
    }
}

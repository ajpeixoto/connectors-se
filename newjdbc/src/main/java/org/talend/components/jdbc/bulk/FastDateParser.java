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
package org.talend.components.jdbc.bulk;

import java.util.HashMap;
import java.util.Locale;

/*
 * This is a copy of routines.system.FastDateParser
 */
public class FastDateParser {

    private static FastDateParser instance;

    public static FastDateParser getInstance() {
        if (instance == null) {
            instance = new FastDateParser();
        }
        return instance;
    }

    private FastDateParser() {
        super();
    }

    private static ThreadLocal<java.util.HashMap<DateFormatKey, java.text.DateFormat>> localCache =
            ThreadLocal.withInitial(HashMap::new);

    private static ThreadLocal<DateFormatKey> localDateFormatKey = ThreadLocal.withInitial(DateFormatKey::new);

    // Warning : DateFormat objects returned by this method are not thread safe
    public static java.text.DateFormat getInstance(String pattern) {
        return getInstance(pattern, null, true);
    }

    public static java.text.DateFormat getInstance(String pattern, boolean lenient) {
        return getInstance(pattern, null, lenient);
    }

    public static java.text.DateFormat getInstance(String pattern, Locale locale) {
        return getInstance(pattern, locale, true);
    }

    public static java.text.DateFormat getInstance(String pattern, Locale locale, boolean lenient) {
        localDateFormatKey.get().pattern = pattern;
        localDateFormatKey.get().locale = locale;
        java.text.DateFormat format = localCache.get().get(localDateFormatKey.get());
        if (format == null) {
            if (pattern.equals("yyyy-MM-dd")) { //$NON-NLS-1$
                format = new DateParser();
            } else if (pattern.equals("yyyy-MM-dd HH:mm:ss")) { //$NON-NLS-1$
                format = new DateTimeParser();
            } else {
                if (locale != null) {
                    format = new java.text.SimpleDateFormat(pattern, locale);
                } else {
                    format = new java.text.SimpleDateFormat(pattern);
                }
            }
            localCache.get().put(new DateFormatKey(pattern, locale), format);
        }
        if (format.isLenient() != lenient) {
            format.setLenient(lenient);
        }
        return format;
    }

    // Parse and format dates with yyyy-MM-dd format
    private static class DateParser extends java.text.DateFormat {

        private int year;

        private int month;

        private int day;

        public DateParser() {
            calendar = java.util.Calendar.getInstance();
        }

        @Override
        public StringBuffer format(java.util.Date date, StringBuffer toAppendTo,
                java.text.FieldPosition fieldPosition) {
            calendar.setTime(date);

            // Year
            toAppendTo.append(calendar.get(java.util.Calendar.YEAR));
            while (toAppendTo.length() < 4)
                toAppendTo.insert(0, "0"); //$NON-NLS-1$
            toAppendTo.append("-"); //$NON-NLS-1$

            // Month
            month = calendar.get(java.util.Calendar.MONTH) + 1;
            if (month < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(month);
            toAppendTo.append("-"); //$NON-NLS-1$

            // Day
            day = calendar.get(java.util.Calendar.DAY_OF_MONTH);
            if (day < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(day);

            return toAppendTo;
        }

        @Override
        public java.util.Date parse(String source, java.text.ParsePosition pos) {
            int index = 0;
            try {
                year = Integer.parseInt(source.substring(0, 4));
                index = 5;
                month = Integer.parseInt(source.substring(5, 7)) - 1;
                index = 8;
                day = Integer.parseInt(source.substring(8, 10));

                pos.setIndex(source.length());

                calendar.clear();
                calendar.set(year, month, day);
                return calendar.getTime();
            } catch (Exception e) {
                pos.setErrorIndex(index);
            }
            return null;
        }
    }

    // Parse dates with yyyy-MM-dd HH:mm:ss format
    private static class DateTimeParser extends java.text.DateFormat {

        private int year;

        private int month;

        private int day;

        private int hour;

        private int minute;

        private int second;

        public DateTimeParser() {
            calendar = java.util.Calendar.getInstance();
        }

        @Override
        public StringBuffer format(java.util.Date date, StringBuffer toAppendTo,
                java.text.FieldPosition fieldPosition) {
            calendar.setTime(date);

            // Year
            toAppendTo.append(calendar.get(java.util.Calendar.YEAR));
            while (toAppendTo.length() < 4)
                toAppendTo.insert(0, "0"); //$NON-NLS-1$
            toAppendTo.append("-"); //$NON-NLS-1$

            // Month
            month = calendar.get(java.util.Calendar.MONTH) + 1;
            if (month < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(month);
            toAppendTo.append("-"); //$NON-NLS-1$

            // Day
            day = calendar.get(java.util.Calendar.DAY_OF_MONTH);
            if (day < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(day);
            toAppendTo.append(" "); //$NON-NLS-1$

            // Hour
            hour = calendar.get(java.util.Calendar.HOUR_OF_DAY);
            if (hour < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(hour);
            toAppendTo.append(":"); //$NON-NLS-1$

            // Minute
            minute = calendar.get(java.util.Calendar.MINUTE);
            if (minute < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(minute);
            toAppendTo.append(":"); //$NON-NLS-1$

            // Second
            second = calendar.get(java.util.Calendar.SECOND);
            if (second < 10)
                toAppendTo.append("0"); //$NON-NLS-1$
            toAppendTo.append(second);

            return toAppendTo;
        }

        @Override
        public java.util.Date parse(String source, java.text.ParsePosition pos) {
            int index = 0;
            try {
                year = Integer.parseInt(source.substring(0, 4));
                index = 5;
                month = Integer.parseInt(source.substring(5, 7)) - 1;
                index = 8;
                day = Integer.parseInt(source.substring(8, 10));
                index = 11;
                hour = Integer.parseInt(source.substring(11, 13));
                index = 14;
                minute = Integer.parseInt(source.substring(14, 16));
                index = 17;
                second = Integer.parseInt(source.substring(17, 19));

                pos.setIndex(source.length());

                calendar.clear();
                calendar.set(year, month, day, hour, minute, second);
                return calendar.getTime();
            } catch (Exception e) {
                pos.setErrorIndex(index);
            }
            return null;
        }
    }

    private static class DateFormatKey {

        private String pattern;

        private Locale locale;

        public DateFormatKey() {
        }

        public DateFormatKey(String pattern, Locale locale) {
            this.pattern = pattern;
            this.locale = locale;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.locale == null) ? 0 : this.locale.hashCode());
            result = prime * result + ((this.pattern == null) ? 0 : this.pattern.hashCode());
            return result;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            final DateFormatKey other = (DateFormatKey) obj;
            if (this.locale == null) {
                if (other.locale != null)
                    return false;
            } else if (!this.locale.equals(other.locale))
                return false;
            if (this.pattern == null) {
                if (other.pattern != null)
                    return false;
            } else if (!this.pattern.equals(other.pattern))
                return false;
            return true;
        }

    }

}

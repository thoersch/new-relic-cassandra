package com.tylerhoersch.nr.cassandra.utility;

import java.util.concurrent.TimeUnit;

public class TimeUnitUtility {

    private static final String EVENTS_DELIMITER = "events/";

    public static Double toMillis(Double sourceValue, TimeUnit sourceUnit) {
        if(sourceUnit == null || sourceValue == null) {
            return null;
        }

        switch (sourceUnit) {
            case DAYS:
                return sourceValue * 86400000;
            case MICROSECONDS:
                return sourceValue * 0.001;
            case HOURS:
                return sourceValue * 3600000;
            case MILLISECONDS:
                return sourceValue;
            case MINUTES:
                return sourceValue * 60000;
            case NANOSECONDS:
                return sourceValue * 1.0e-6;
            case SECONDS:
                return sourceValue * 1000;
            default:
                return sourceValue;
        }
    }

    public static TimeUnit cleanUnitsString(String units) {
        if(units == null) {
            return TimeUnit.SECONDS;
        }

        if(units.contains(EVENTS_DELIMITER)) {
            units = units.substring(EVENTS_DELIMITER.length());
            units += "s";
        }

        return TimeUnit.valueOf(units);
    }
}

package com.tylerhoersch.nr.cassandra.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FormattedSizeUtility {
    private final static Logger logger = LoggerFactory.getLogger(FormattedSizeUtility.class);
    private final static long KB_FACTOR = 1000;
    private final static long KIB_FACTOR = 1024;
    private final static long MB_FACTOR = 1000 * KB_FACTOR;
    private final static long MIB_FACTOR = 1024 * KIB_FACTOR;
    private final static long GB_FACTOR = 1000 * MB_FACTOR;
    private final static long GIB_FACTOR = 1024 * MIB_FACTOR;

    public static double parse(String formatted) {
        if(formatted == null || formatted == "") {
            return 0;
        }

        try {
            int space = formatted.indexOf(" ");
            double num = Double.parseDouble(formatted.substring(0, space));
            String unit = formatted.substring(space + 1);
            switch (unit) {
                case "GB":
                    return num * GB_FACTOR;
                case "GiB":
                    return num * GIB_FACTOR;
                case "MB":
                    return num * MB_FACTOR;
                case "MiB":
                    return num * MIB_FACTOR;
                case "KB":
                    return num * KB_FACTOR;
                case "KiB":
                    return num * KIB_FACTOR;
                default:
                    return 0;
            }
        } catch (Exception e) {
            logger.error(String.format("Trying to convert '%s' to a bytes", formatted), e);
            return 0;
        }
    }
}

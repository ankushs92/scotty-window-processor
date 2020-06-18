package de.tub.dima.scotty.slicing.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeUtil {

    public static ZonedDateTime parse(final long ms) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(ms), ZoneId.of("UTC"));
    }
}

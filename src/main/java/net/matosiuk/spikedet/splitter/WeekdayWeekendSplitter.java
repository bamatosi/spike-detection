package net.matosiuk.spikedet.splitter;

import java.io.Serializable;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.function.Function;

public class WeekdayWeekendSplitter {
    public static Function<Long,Byte> getSplitter() {
        return (Function<Long,Byte> & Serializable) l -> {
            LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochSecond(l), TimeZone.getTimeZone("Europe/London").toZoneId());

            DayOfWeek wDay = date.getDayOfWeek();
            if ((wDay.compareTo(DayOfWeek.SATURDAY) == 0) || (wDay.compareTo(DayOfWeek.SUNDAY) == 0)) {
                return (byte)2;
            } else {
                return (byte)1;
            }
        };
    }
}

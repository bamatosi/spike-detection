import net.matosiuk.spikedet.splitter.WeekdayWeekendSplitter;
import org.junit.*;

import java.time.Instant;
import java.util.function.Function;

public class SplitterTest {
    @Test
    public void weekdayWeekendSplitterTest() {

        Function<Long,Byte> splitter = WeekdayWeekendSplitter.getSplitter();

        Instant week = Instant.parse("2016-02-08T00:00:01Z");
        byte weekClass = splitter.apply(week.getEpochSecond());
        Assert.assertEquals(weekClass, 1);

        week = Instant.parse("2016-02-09T12:00:00Z");
        weekClass = splitter.apply(week.getEpochSecond());
        Assert.assertEquals(weekClass, 1);

        week = Instant.parse("2016-02-10T12:01:00Z");
        weekClass = splitter.apply(week.getEpochSecond());
        Assert.assertEquals(weekClass, 1);

        week = Instant.parse("2016-02-11T12:01:00Z");
        weekClass = splitter.apply(week.getEpochSecond());
        Assert.assertEquals(weekClass, 1);

        week = Instant.parse("2016-02-12T23:59:59Z");
        weekClass = splitter.apply(week.getEpochSecond());
        Assert.assertEquals(weekClass, 1);

        Instant weekend = Instant.parse("2016-02-06T12:00:00Z");
        byte weekendClass = splitter.apply(weekend.getEpochSecond());
        Assert.assertEquals(weekendClass,2);

        weekend = Instant.parse("2016-02-07T12:00:00Z");
        weekendClass = splitter.apply(weekend.getEpochSecond());
        Assert.assertEquals(weekendClass,2);
    }
}

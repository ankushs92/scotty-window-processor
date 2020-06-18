package de.tub.dima.scotty.slicing;

import de.tub.dima.scotty.core.AggregateWindow;
import de.tub.dima.scotty.core.TimeMeasure;
import de.tub.dima.scotty.core.windowFunction.AggregateFunction;
import de.tub.dima.scotty.core.windowType.SlidingWindow;
import de.tub.dima.scotty.core.windowType.WindowMeasure;
import de.tub.dima.scotty.state.memory.MemoryStateFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class Test {
    private static MemoryStateFactory stateFactory = new MemoryStateFactory();

    private static SlicingWindowOperator<Double> slicingWindowOperator = new SlicingWindowOperator<>(stateFactory);

    public static void main(String[] args) {
        slicingWindowOperator.addWindowFunction(new AggregateFunction<Double, Integer, Integer>() {
            @Override
            public Integer lift(Double inputTuple) {
                return 1;
            }

            @Override
            public Integer combine(Integer partialAggregate1, Integer partialAggregate2) {
                return partialAggregate1 + partialAggregate2;
            }

            @Override
            public Integer lower(Integer aggregate) {
                System.out.println(aggregate);
                return aggregate;
            }
        });
        slicingWindowOperator.addWindowAssigner(new SlidingWindow(WindowMeasure.Time,
                TimeMeasure.seconds(10).toMilliseconds(),
                TimeMeasure.seconds(1).toMilliseconds()));
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        long l = now.toInstant().toEpochMilli();
        System.out.println(l);
        // 1,10 ; 5-15; 20-30---
        slicingWindowOperator.processElement(1.0, l);
        slicingWindowOperator.processElement(1.0, l);

        slicingWindowOperator.processElement(2.0, l);
        slicingWindowOperator.processElement(3.0, l);
        slicingWindowOperator.processElement(4.0, l);
        slicingWindowOperator.processElement(5.0, l);

        List<AggregateWindow> resultWindows = slicingWindowOperator.processWatermark(now.plus(1, ChronoUnit.SECONDS).toInstant().toEpochMilli());
        System.out.println(resultWindows);

        ZonedDateTime plus = now.plus(2, ChronoUnit.SECONDS);
        slicingWindowOperator.processElement(5.0, plus.toInstant().toEpochMilli());
        slicingWindowOperator.processElement(5.0, plus.toInstant().toEpochMilli());
        slicingWindowOperator.processElement(5.0, plus.toInstant().toEpochMilli());
        slicingWindowOperator.processElement(5.0, plus.toInstant().toEpochMilli());
        slicingWindowOperator.processElement(5.0, plus.toInstant().toEpochMilli());
        slicingWindowOperator.processElement(5.0, plus.toInstant().toEpochMilli());

        resultWindows = slicingWindowOperator.processWatermark(plus.plus(1, ChronoUnit.SECONDS).toInstant().toEpochMilli());
        System.out.println(resultWindows);

    }


}

package tech.ydb.apps;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class Ticker {
    public class Measure implements AutoCloseable {
        private final Method method;
        private final long startedAt;
        private long count = 0;

        public Measure(Method method) {
            this.method = method;
            this.startedAt = System.currentTimeMillis();
        }

        public void inc() {
            inc(1);
        }

        public void inc(long value) {
            count += value;
        }

        @Override
        public void close() {
            if (count == 0) {
                return;
            }

            long ms = System.currentTimeMillis() - startedAt;

            method.count.add(count);
            method.totalCount.add(count);

            method.timeMs.add(ms);
            method.totalTimeMs.add(ms);
        }
    }

    public class Method {
        private final String name;

        private final LongAdder totalCount = new LongAdder();
        private final LongAdder totalTimeMs = new LongAdder();

        private final LongAdder count = new LongAdder();
        private final LongAdder timeMs = new LongAdder();

        private volatile long lastPrinted = 0;

        public Method(String name) {
            this.name = name;
        }

        public Measure newCall() {
            return new Measure(this);
        }

        public void print(Logger logger) {
            if (count.longValue() > 0 && lastPrinted != 0) {
                long ms = System.currentTimeMillis() - lastPrinted;
                double rps = 1000 * count.longValue() / ms;
                logger.info("Op {} executed {} times with RPS {} ops", name, count.longValue(), rps);
            }

            count.reset();
            timeMs.reset();
            lastPrinted = System.currentTimeMillis();
        }

        public void printTotal(Logger logger) {
            if (totalCount.longValue() > 0) {
                double average = 1.0d  * totalTimeMs.longValue() / totalCount.longValue();
                logger.info("Op {} executed {} times, with average time {} ms/op", name, totalCount.longValue(), average);
            }
        }
    }

    private final Logger logger;
    private final Method load = new Method("LOAD");

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "ticker")
    );

    public Ticker(Logger logger) {
        this.logger = logger;
    }

    public Method getLoad() {
        return this.load;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::print, 1, 10, TimeUnit.SECONDS);
    }

    public void stop() throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(20, TimeUnit.SECONDS);
    }

    private void print() {
        logger.info("------------ INFO ----------------");
        Arrays.asList(load).forEach(m -> m.print(logger));
    }

    public void printTotal() {
        logger.info("=========== TOTAL ==============");
        Arrays.asList(load).forEach(m -> m.printTotal(logger));
    }
}

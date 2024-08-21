package tech.ydb.apps;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

import tech.ydb.apps.service.SchemeService;
import tech.ydb.apps.service.TokenService;

/**
 *
 * @author Aleksandr Gorshenin
 */
@EnableRetry
@SpringBootApplication
public class Application implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private static final int THREADS_COUNT = 4;
    private static final int RECORDS_COUNT = 100_000;
    private static final int LOAD_BATCH_SIZE = 1000;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    private final SchemeService schemeService;
    private final TokenService tokenService;

    private final ExecutorService executor;
    private final AtomicInteger threadCounter = new AtomicInteger(0);

    public Application(SchemeService schemeService, TokenService tokenService) {
        this.schemeService = schemeService;
        this.tokenService = tokenService;

        this.executor = Executors.newFixedThreadPool(THREADS_COUNT, this::threadFactory);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("CLI app has started");
        for (String arg : args) {
            logger.info("execute {} step", arg);

            if ("clean".equalsIgnoreCase(arg)) {
                schemeService.executeClean();
            }

            if ("init".equalsIgnoreCase(arg)) {
                schemeService.executeInit();
            }

            if ("load".equalsIgnoreCase(arg)) {
                loadData();
            }
        }

        logger.info("CLI app is waiting for finishing");

        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.MINUTES);

        logger.info("CLI app has finished");
    }

    private Thread threadFactory(Runnable runnable) {
        return new Thread(runnable, "app-thread-" + threadCounter.incrementAndGet());
    }

    private void loadData() {
        int id = 0;
        while (id < RECORDS_COUNT) {
            final int first = id;
            id += LOAD_BATCH_SIZE;
            final int last = id < RECORDS_COUNT ? id : RECORDS_COUNT;

            executor.submit(() -> {
                logger.info("load batch {} - {}", first, last);
                tokenService.insertBatch(first, last);
                logger.info("load batch {} - {} OK", first, last);
            });
        }
    }
}

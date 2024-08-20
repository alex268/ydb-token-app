package tech.ydb.apps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

import tech.ydb.apps.service.SchemeService;

/**
 *
 * @author Aleksandr Gorshenin
 */
@EnableRetry
@SpringBootApplication
public class Application implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args).close();
    }

    private final SchemeService schemeService;

    public Application(SchemeService schemeService) {
        this.schemeService = schemeService;
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("Start CLI app");
        for (String arg : args) {
            logger.info("execute {} step", arg);
            if ("clean".equalsIgnoreCase(arg)) {
                schemeService.executeClean();
            }

            if ("init".equalsIgnoreCase(arg)) {
                schemeService.executeInit();
            }
        }
    }
}

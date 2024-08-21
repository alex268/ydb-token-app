package tech.ydb.apps.service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;

import javax.persistence.EntityManager;

import com.google.common.io.Files;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.ResourceUtils;

import tech.ydb.apps.annotation.YdbRetryable;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Service
public class SchemeService {
    private static final Logger logger = LoggerFactory.getLogger(SchemeService.class);

    private final EntityManager em;

    public SchemeService(EntityManager em) {
        this.em = em;
    }

    @Transactional
    @YdbRetryable
    public void executeClean() {
        String script = getDropSqlScript();
        if (script == null) {
            logger.warn("cannot find drop sql in classpath");
            return;
        }


        // TODO: Add support of DDL queries to prepareStatement
        em.unwrap(Session.class).doWork(connection -> {
            try (Statement st = connection.createStatement()) {
                st.execute(script);
                logger.info("tables have been dropped");
            } catch (SQLException ex) {
                logger.warn("cannot drop tables with message {}", ex.getMessage());
            }
        });
    }

    @Transactional
    @YdbRetryable
    public void executeInit() {
        String script = getInitSqlScript();
        if (script == null) {
            logger.warn("cannot find init sql in classpath");
            return;
        }


        // TODO: Add support of DDL queries to prepareStatement
        em.unwrap(Session.class).doWork(connection -> {
            try (Statement st = connection.createStatement()) {
                st.execute(script);
                logger.info("tables have been created");
            }
        });
    }

    private String getDropSqlScript() {
        try {
            File file = ResourceUtils.getFile("classpath:sql/drop.sql");
            return Files.asCharSource(file, StandardCharsets.UTF_8).read();
        } catch (IOException e) {
            return null;
        }
    }

    private String getInitSqlScript() {
        try {
            File file = ResourceUtils.getFile("classpath:sql/init.sql");
            return Files.asCharSource(file, StandardCharsets.UTF_8).read();
        } catch (IOException e) {
            return null;
        }
    }
}

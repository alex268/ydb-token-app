package tech.ydb.apps.service;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;

import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tech.ydb.apps.annotation.YdbRetryable;
import tech.ydb.apps.entity.Token;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Service
public class TokenService {
    private final static Logger logger = LoggerFactory.getLogger(TokenService.class);
    private final EntityManager em;

    public TokenService(EntityManager em) {
        this.em = em;
    }

    @YdbRetryable
    @Transactional
    public void insertBatch(int firstID, int lastID) {
        for (int id = firstID; id < lastID; id++) {
            Token token = new Token("user_" + id);
            em.persist(token);
        }
    }

    @YdbRetryable
    @Transactional
    public Token fetchToken(int id) {
        String username = "user_" + id;
        for (int version = 1; version < 20; version ++) {
            Token token = em.find(Token.class, Token.getKey(username));
            logger.trace("finded token {}/{} -> {}", username, version, token);
            if (token != null) {
                return token;
            }
        }

        logger.warn("token {} is not found", username);
        return null;
    }

    @YdbRetryable
    @Transactional
    public void updateToken(int id) {
        Token token = fetchToken(id);
        if (token != null) {
            token.incVersion();
            em.merge(token);
            logger.trace("updated token {} -> {}", id, token.getVersion());
        }
    }

    @YdbRetryable
    @Transactional
    public void updateBatch(List<Integer> ids) {
        List<Token> batch = new ArrayList<>();
        for (Integer id: ids) {
            Token token = fetchToken(id);
            if (token != null) {
                logger.info("update token {}", token);
                token.incVersion();
                batch.add(token);
            }
        }

        String query = ""
                + "DECLARE $batch AS List<Struct<p1: Text, p2: Int32>>;"
                + "UPDATE app_token ON SELECT p1 as id, p2 as version FROM AS_TABLE($batch)";
        em.unwrap(Session.class).doWork(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(query)) {
                for (Token t: batch) {
                    ps.setString(1, t.getId().toString()); // p1 == id
                    ps.setInt(2, t.getVersion());          // p2 == version
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        });

        for (Token t: batch) {
            em.detach(t); // or em.refresh(t)
        }
    }
}

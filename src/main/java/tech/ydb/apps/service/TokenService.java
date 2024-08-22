package tech.ydb.apps.service;

import javax.persistence.EntityManager;

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
            Token token = new Token("user_" + id, 1);
            em.persist(token);
        }
    }

    @YdbRetryable
    @Transactional
    public Token fetchToken(int id) {
        String username = "user_" + id;
        for (int version = 1; version < 20; version ++) {
            Token token = em.find(Token.class, Token.getKey(username, version).toString());
            logger.trace("finded token {}/{} -> {}", username, version, token);
            if (token != null) {
                return token;
            }
        }

        return null;
    }

    @YdbRetryable
    @Transactional
    public void updateToken(int id) {
        Token previos = fetchToken(id);
        if (previos != null) {
            Token updated = new Token(previos.getUserName(), previos.getVersion() + 1);
            em.persist(updated);
            em.remove(previos);
            logger.trace("updated token {}, {} -> {}", id, previos.getVersion(), updated.getVersion());
        }
    }
}

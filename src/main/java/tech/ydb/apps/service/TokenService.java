package tech.ydb.apps.service;

import javax.persistence.EntityManager;

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
    private final EntityManager em;

    public TokenService(EntityManager em) {
        this.em = em;
    }

    @Transactional
    @YdbRetryable
    public void insertBatch(int firstID, int lastID) {
        for (int id = firstID; id < lastID; id++) {
            Token token = new Token("user_" + id, 1);
            em.persist(token);
        }
    }
}

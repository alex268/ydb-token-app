package tech.ydb.apps.repo;

import java.util.UUID;

import org.springframework.data.repository.CrudRepository;

import tech.ydb.apps.entity.Token;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface TokenRepository extends CrudRepository<Token, UUID> {

    void saveAllAndFlush(Iterable<Token> list);

}

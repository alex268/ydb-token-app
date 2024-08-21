package tech.ydb.apps.entity;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Entity
@Table(name = "app_token")
public class Token {
    @Id
    private UUID id;

    @Column
    private String username;

    @Column
    private Integer version;

    public UUID getId() {
        return this.id;
    }

    public String getUserName() {
        return this.username;
    }

    public Integer getVersion() {
        return this.version;
    }

    public Token() { }

    public Token(String username, int version) {
        // UUID based on MD5 hash
        this.id = UUID.nameUUIDFromBytes((username + "_v" + version).getBytes());
        this.username = username;
        this.version = version;
    }

    @Override
    public String toString() {
        return "Token{id=" + id.toString() + ", username='" + username + "', version=" + version + "}";
    }
}

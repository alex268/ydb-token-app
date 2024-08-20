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
}

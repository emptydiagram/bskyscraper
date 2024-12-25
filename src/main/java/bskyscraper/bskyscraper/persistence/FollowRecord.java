package bskyscraper.bskyscraper.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Data;


@Data
@Entity
@Table(name = "follow_record")
@IdClass(FollowRecordId.class)
public class FollowRecord {

    @Id
    @Column(name = "user_did", nullable = false)
    private String userDid;

    @Id
    @Column(name = "rkey", nullable = false)
    private String rkey;

    @Column(name = "subject_did", nullable = false)
    private String subjectDid;
}
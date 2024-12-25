package bskyscraper.bskyscraper.persistence;

import java.io.Serializable;

import jakarta.persistence.Embeddable;
import lombok.Data;

@Data
@Embeddable
public class FollowRecordId implements Serializable {
    private String userDid;
    private String rkey;

    public FollowRecordId() {}

    public FollowRecordId(String userDid, String rkey) {
        this.userDid = userDid;
        this.rkey = rkey;
    }
}
package bskyscraper.bskyscraper.dto;

import lombok.Data;

@Data
public class FollowCommit {
    private String rev;
    private String operation;
    private String collection;
    private String rkey;
    private FollowRecord record;
    private String cid;
}
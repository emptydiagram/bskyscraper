package bskyscraper.model;

import lombok.Data;

@Data
public class Commit {

    private String rev;
    private String operation;
    private String collection;
    private String rkey;
    private Record record;

}
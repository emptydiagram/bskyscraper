package bskyscraper.bskyscraper.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class FollowRecord {

    @JsonProperty("$type")
    private String type;

    private String createdAt;

    private String subject;

}
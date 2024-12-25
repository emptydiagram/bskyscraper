package bskyscraper.bskyscraper.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class FollowEvent {

    private String did;

    @JsonProperty("time_us")
    private long timeUs;

    private String kind;

    private FollowCommit commit;

}

package bskyscraper.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class JetstreamEvent {

    private String did;

    @JsonProperty("time_us")
    private long timeUs;

    private String kind;

    private Commit commit;

    // TODO: identity, account?

}

package bskyscraper.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Record {

    @JsonProperty("$type")
    private String type;

    private String createdAt;

    private List<String> langs;

    private String text;

}
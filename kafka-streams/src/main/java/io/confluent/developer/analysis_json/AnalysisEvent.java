package io.confluent.developer.analysis_json;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Properties;

public class AnalysisEvent {
    @JsonProperty
    public long created;

    @JsonProperty
    public String type;

    @JsonProperty
    public String name;

    @JsonProperty
    public Properties properties;

    public AnalysisEvent(long created, String type, String name, Properties properties) {
        this.created = created;
        this.type = type;
        this.name = name;
        this.properties = properties;
    }

}

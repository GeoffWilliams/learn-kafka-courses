package io.confluent.developer.analysis_json;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AnalysisEvent {
    @JsonProperty
    public long created;

    @JsonProperty
    public String type;

    @JsonProperty
    public String name;

    public AnalysisEvent(long created, String type, String name) {
        this.created = created;
        this.type = type;
        this.name = name;
    }

}

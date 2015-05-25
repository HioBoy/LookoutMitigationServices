package com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Builder;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JsMitigationGroup {
    @Getter
    @Setter
    @JsonProperty("Description")
    private String description;

    @Getter @Setter
    @JsonProperty("Active")
    private Boolean active;

    @Getter @Setter
    @JsonProperty("POPs")
    private List<String> pops;

    @Getter @Setter
    @JsonProperty("Hosts")
    private List<String> hosts;

    @Getter @Setter
    @JsonProperty("Mitigations")
    private List<String> mitigations;
}

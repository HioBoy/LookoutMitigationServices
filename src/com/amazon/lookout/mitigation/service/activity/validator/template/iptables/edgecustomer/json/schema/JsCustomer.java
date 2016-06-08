package com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Builder;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JsCustomer {
    @Getter
    @Setter
    @JsonProperty("Mitigation-Groups")
    private List<JsMitigationGroup> mitigationGroups;
}

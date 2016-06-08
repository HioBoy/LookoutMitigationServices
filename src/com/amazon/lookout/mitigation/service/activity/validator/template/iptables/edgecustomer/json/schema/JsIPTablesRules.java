package com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.Builder;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JsIPTablesRules {
    @Getter
    @Setter
    @JsonProperty("Nonce")
    private Long nonce;

    @Getter @Setter
    @JsonProperty("Customers")
    private Map<String, JsCustomer> customers;

    @Getter @Setter
    @JsonProperty("Mitigation-Definitions")
    private Map<String, Object> mitigationDefinitions;

    @Getter @Setter
    @JsonProperty("Custom-Country-Codes")
    private Object customCountryCodes;
}

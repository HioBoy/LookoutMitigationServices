package com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer;

import com.amazon.coral.google.common.collect.ImmutableList;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema.JsCustomer;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema.JsIPTablesRules;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema.JsMitigationGroup;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.Validate;

import java.util.Map;

public class IPTablesJsonValidator {
    private static final ImmutableList<String> requiredCustomers = ImmutableList.of(
            "Route53", "CloudFront", "CloudFront-DNS", "CloudFront-Metro");

    public void validateIPTablesJson(String ipTablesJson) {
        if (ipTablesJson == null) {
            throw new IllegalArgumentException("IPTables JSON cannot be null or empty");
        }

        Validate.notEmpty(ipTablesJson, "IPTables JSON cannot be null or empty");
        Validate.isTrue(
                ipTablesJson.length() <= 28_000,
                "IPTables JSON length should be less than or equal to 28,000. Consider removing unused mitigations. " +
                        "Actual length: " + ipTablesJson.length());

        JsIPTablesRules ipTablesRules = parseIPTablesJson(ipTablesJson);

        if (ipTablesRules.getNonce() == null) {
            throw new IllegalArgumentException(
                    "Nonce top level field is missing. Please add \"Nonce\":0 to the top level object.");
        }

        if (ipTablesRules.getCustomers() == null) {
            throw new IllegalArgumentException("Customers top level element is missing.");
        }

        if (ipTablesRules.getMitigationDefinitions() == null) {
            throw new IllegalArgumentException("Mitigation-Definitions top level element is missing.");
        }

        validateCustomers(ipTablesRules.getCustomers());
    }

    private void validateCustomers(Map<String, JsCustomer> customers) {
        requiredCustomers.stream()
                .filter(requiredCustomer -> !customers.containsKey(requiredCustomer))
                .findFirst()
                .ifPresent(missingCustomer -> {
                    throw new IllegalArgumentException(
                            "Required customer " + missingCustomer + " is missing in element Customers.");
                });

        for (Map.Entry<String, JsCustomer> customer : customers.entrySet()) {
            validateCustomer(customer.getKey(), customer.getValue());
        }
    }

    public void validateCustomer(String customerName, JsCustomer customer) {
        if (customer.getMitigationGroups() != null) {
            for (JsMitigationGroup mitigationGroup : customer.getMitigationGroups()) {
                validateMitigationGroup(mitigationGroup, customerName);
            }
        }
    }

    public void validateMitigationGroup(JsMitigationGroup mitigationGroup, String customerName) {
        Validate.notBlank(
                mitigationGroup.getDescription(),
                String.format(
                        "Description field is required in %s.Mitigation-Groups: %s",
                        customerName,
                        mitigationGroup.toString()));

        if (mitigationGroup.getActive() == null) {
            throw new IllegalArgumentException(String.format(
                    "Active field is required in %s.Mitigation-Groups: %s",
                    customerName,
                    mitigationGroup.toString()));
        }

        if (mitigationGroup.getPops() == null) {
            throw new IllegalArgumentException(String.format(
                    "POPs field is required in %s.Mitigation-Groups: %s",
                    customerName,
                    mitigationGroup.toString()));
        }

        if (mitigationGroup.getMitigations() == null) {
            throw new IllegalArgumentException(String.format(
                    "Mitigations field is required in %s.Mitigation-Groups: %s",
                    customerName,
                    mitigationGroup.toString()));
        }
    }

    private JsIPTablesRules parseIPTablesJson(String ipTablesJson) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(ipTablesJson, JsIPTablesRules.class);
        } catch (JsonParseException e) {
            throw new IllegalArgumentException("Unable to parse IPTables JSON: " + e.getMessage(), e);
        } catch (Throwable e) {
            throw new IllegalArgumentException("Unable to parse IPTables JSON.", e);
        }
    }
}

package com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer;

import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema.JsCustomer;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.json.schema.JsMitigationGroup;
import com.google.common.collect.Lists;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.amazon.lookout.mitigation.service.utils.AssertUtils.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

@RunWith(JUnitParamsRunner.class)
public class IPTablesJsonValidatorTest {
    @Test
    public void validateCustomerValidatesMitigationGroup() {
        IPTablesJsonValidator validator = new IPTablesJsonValidator();
        JsCustomer customer = JsCustomer.builder()
                .mitigationGroups(
                        Lists.newArrayList(
                                validMitigationGroup()
                                        .description(null)
                                        .build()))
                .build();

        NullPointerException actualError = assertThrows(
                NullPointerException.class,
                () -> validator.validateCustomer("Route53", customer));

        assertThat(actualError.getMessage(), containsString("Description"));
    }

    @Test
    @Parameters({
            "",
            " ",
            "\n",
            "\t"
    })
    public void validateMitigationGroupWithInvalidDescription(String invalidDescription) {
        IPTablesJsonValidator validator = new IPTablesJsonValidator();
        JsMitigationGroup mitigationGroup = validMitigationGroup()
                .description(invalidDescription)
                .build();

        IllegalArgumentException actualError = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateMitigationGroup(mitigationGroup, "Route53"));

        assertThat(actualError.getMessage(), containsString("Description"));
    }

    @Test
    public void validateMitigationGroupMissingActive() {
        IPTablesJsonValidator validator = new IPTablesJsonValidator();
        JsMitigationGroup mitigationGroup = validMitigationGroup()
                .active(null)
                .build();

        IllegalArgumentException actualError = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateMitigationGroup(mitigationGroup, "Route53"));

        assertThat(actualError.getMessage(), containsString("Active"));
    }

    @Test
    public void validateMitigationGroupMissingPOPs() {
        IPTablesJsonValidator validator = new IPTablesJsonValidator();
        JsMitigationGroup mitigationGroup = validMitigationGroup()
                .pops(null)
                .build();

        IllegalArgumentException actualError = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateMitigationGroup(mitigationGroup, "Route53"));

        assertThat(actualError.getMessage(), containsString("POPs"));
    }

    @Test
    public void validateMitigationGroupMissingMitigations() {
        IPTablesJsonValidator validator = new IPTablesJsonValidator();
        JsMitigationGroup mitigationGroup = validMitigationGroup()
                .mitigations(null)
                .build();

        IllegalArgumentException actualError = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateMitigationGroup(mitigationGroup, "Route53"));

        assertThat(actualError.getMessage(), containsString("Mitigations"));
    }

    private JsMitigationGroup.JsMitigationGroupBuilder validMitigationGroup() {
        return JsMitigationGroup.builder()
                .description("TestDescription")
                .active(true)
                .pops(Lists.newArrayList())
                .hosts(Lists.newArrayList())
                .mitigations(Lists.newArrayList());
    }
}

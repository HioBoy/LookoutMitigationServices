package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.coral.google.common.collect.ImmutableList;
import com.amazon.lookout.mitigation.service.ApplyIPTablesRulesAction;
import com.amazon.lookout.mitigation.service.CompositeAndConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.IPTablesJsonValidator;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.google.common.collect.Lists;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(JUnitParamsRunner.class)
public class IPTablesEdgeCustomerValidatorTest {
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure(Level.OFF);
    }

    @Test
    public void validateRequestForTemplateIsSuccessful() {
        CreateMitigationRequest request = generateCreateMitigationRequest(validIpTablesJson());
        IPTablesEdgeCustomerValidator validator = createValidator();

        validator.validateRequestForTemplate(request, request.getMitigationTemplate());
    }

    @Test
    public void validateRequestForTemplateValidatesIPTablesRulesForCreateRequest() throws Exception {
        IllegalArgumentException expectedError = new IllegalArgumentException("expected exception");
        IPTablesJsonValidator ipTablesJsonValidator = mock(IPTablesJsonValidator.class);
        doThrow(expectedError)
                .when(ipTablesJsonValidator)
                .validateIPTablesJson(validIpTablesJson());
        CreateMitigationRequest request = generateCreateMitigationRequest(validIpTablesJson());
        IPTablesEdgeCustomerValidator validator = new IPTablesEdgeCustomerValidator(ipTablesJsonValidator);

        IllegalArgumentException actualError = assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateRequestForTemplate(request, request.getMitigationTemplate()));

        assertThat(actualError, is(expectedError));
    }

    @Test
    @Parameters({
            "",
            "Some\nName!",
            "Some\rName!",
            "Some\r\nName!",
            "Some\u2028Name!"})
    public void validateRequestForTemplateWithBadMitigationName(String invalidMitigationName) throws Exception {
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationName(invalidMitigationName);

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, request);

        assertThat(actualError.getMessage(), containsString("mitigationName"));
    }

    @Test
    public void validateRequestForTemplateWithNullMitigationName() throws Exception {
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationName(null);

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, request);

        assertThat(actualError.getMessage(), containsString("mitigationName"));
    }

    @Test
    public void validateRequestForTemplateWhenMitigationDefinitionIsNull() throws Exception {
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationDefinition(null);

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, request);

        assertThat(actualError.getMessage(), containsString("mitigationDefinition"));
    }

    @Test
    public void validateRequestForTemplateWhenConstraintHasInvalidType() throws Exception {
        Constraint invalidConstraint = new CompositeAndConstraint();
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationDefinition(generateMitigationDefinition(invalidConstraint));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, request);

        assertThat(actualError.getMessage(), containsString("constraint"));
    }

    @Test
    public void validateRequestForTemplateWhenConstraintAttributeValueIsNull() throws Exception {
        SimpleConstraint ipTablesConstraint = new SimpleConstraint();
        ipTablesConstraint.setAttributeName("IP_TABLES_RULES");
        ipTablesConstraint.setAttributeValues(null);
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationDefinition(generateMitigationDefinition(ipTablesConstraint));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, request);

        assertThat(actualError.getMessage(), containsString("constraint"));
    }

    @Test
    public void validateRequestForTemplateWhenConstraintAttributeValueIsEmpty() throws Exception {
        SimpleConstraint ipTablesConstraint = new SimpleConstraint();
        ipTablesConstraint.setAttributeName("IP_TABLES_RULES");
        ipTablesConstraint.setAttributeValues(Collections.emptyList());
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationDefinition(generateMitigationDefinition(ipTablesConstraint));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, request);

        assertThat(actualError.getMessage(), containsString("constraint"));
    }

    @Test
    public void validateRequestForTemplateWhenIPTablesJsonIsEmpty() throws Exception {
        String ipTablesJson = "";

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, ipTablesJson);

        assertThat(actualError.getMessage(), containsString("IPTables JSON"));
    }

    @Test
    public void validateRequestForTemplateWhenIPTablesJsonIsMalformed() throws Exception {
        String ipTablesJson = ipTablesMalformedJson();

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, ipTablesJson);

        assertThat(actualError.getMessage(), containsString("IPTables JSON"));
    }

    @Test
    public void validateRequestForTemplateWhenIPTablesJsonHasNoNonce() throws Exception {
        String ipTablesJson = validIpTablesJson().replace(
                elementTitle("Nonce"),
                elementTitle("N-o-n-c-e"));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, ipTablesJson);

        assertThat(actualError.getMessage(), containsString("Nonce"));
    }

    @Test
    public void validateRequestForTemplateWhenIPTablesJsonHasNoCustomers() throws Exception {
        String ipTablesJson = validIpTablesJson().replace(
                elementTitle("Customers"),
                elementTitle("C-u-s-t-o-m-e-r-s"));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, ipTablesJson);

        assertThat(actualError.getMessage(), containsString("Customers"));
    }

    @Test
    public void validateRequestForTemplateWhenIPTablesJsonHasNoMitigationDefinitions() throws Exception {
        String ipTablesJson = validIpTablesJson().replace(
                elementTitle("Mitigation-Definitions"),
                elementTitle("M-itigation-D-efinitions"));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, ipTablesJson);

        assertThat(actualError.getMessage(), containsString("Mitigation-Definitions"));
    }

    @Test
    @Parameters({
            "Route53",
            "CloudFront",
            "CloudFront-DNS",
            "CloudFront-Metro"
    })
    public void validateRequestForTemplateWhenIPTablesJsonMissingRequiredCustomers(String requiredCustomer)
        throws Exception {

        String ipTablesJson = validIpTablesJson().replace(
                elementTitle(requiredCustomer),
                elementTitle("-" + requiredCustomer + "-"));

        IllegalArgumentException actualError = assertValidationThrows(IllegalArgumentException.class, ipTablesJson);

        assertThat(actualError.getMessage(), containsString(requiredCustomer));
    }

    @Test
    public void validateCoexistenceForTemplateAndDeviceForSameTemplate() throws Exception {
        IPTablesEdgeCustomerValidator validator = createValidator();
        MitigationDefinition definition1 = generateMitigationDefinition(validIpTablesJson());
        MitigationDefinition definition2 = generateMitigationDefinition(validIpTablesJson());

        DuplicateDefinitionException400 actualError = assertThrows(
                DuplicateDefinitionException400.class,
                () -> validator.validateCoexistenceForTemplateAndDevice(
                        MitigationTemplate.IPTables_Mitigation_EdgeCustomer,
                        "Mitigation1",
                        definition1,
                        MitigationTemplate.IPTables_Mitigation_EdgeCustomer,
                        "Mitigation2",
                        definition2));

        assertThat(actualError.getMessage(), containsString(MitigationTemplate.IPTables_Mitigation_EdgeCustomer));
        assertThat(actualError.getMessage(), containsString("Mitigation2"));
    }

    private String elementTitle(String elementName) {
        return "\"" + elementName + "\":";
    }

    private static <T extends Throwable> T assertValidationThrows(Class<T> expectedException, String ipTablesJson)
        throws Exception {

        CreateMitigationRequest request = generateCreateMitigationRequest(ipTablesJson);
        return assertValidationThrows(expectedException, request);
    }

    private static <T extends Throwable> T assertValidationThrows(
            Class<T> expectedException, CreateMitigationRequest request) throws Exception {

        IPTablesEdgeCustomerValidator validator = createValidator();
        return assertThrows(
                expectedException,
                () -> validator.validateRequestForTemplate(request, request.getMitigationTemplate()));
    }

    private static CreateMitigationRequest generateCreateMitigationRequest() {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("TestIPTablesMitigation");
        request.setServiceName(ServiceName.Edge);
        request.setMitigationTemplate(MitigationTemplate.IPTables_Mitigation_EdgeCustomer);
        request.setLocations(Lists.newArrayList(StandardLocations.EDGE_WORLD_WIDE));

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        MitigationDefinition mitigationDefinition = generateMitigationDefinition(new SimpleConstraint());
        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }

    private static CreateMitigationRequest generateCreateMitigationRequest(String ipTablesJson) {
        CreateMitigationRequest request = generateCreateMitigationRequest();
        request.setMitigationDefinition(generateMitigationDefinition(ipTablesJson));
        return request;
    }

    private static MitigationDefinition generateMitigationDefinition(Constraint constraint) {
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new ApplyIPTablesRulesAction());
        mitigationDefinition.setConstraint(constraint);
        return mitigationDefinition;
    }

    private static MitigationDefinition generateMitigationDefinition(String ipTablesJson) {
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName("IP_TABLES_RULES");
        constraint.setAttributeValues(ImmutableList.of(ipTablesJson));
        return generateMitigationDefinition(constraint);
    }

    private static IPTablesEdgeCustomerValidator createValidator() {
        return new IPTablesEdgeCustomerValidator(new IPTablesJsonValidator());
    }

    private static String ipTablesMalformedJson() {
        return "{\n" +
                "    \"Nonce\":0,\n" +
                "    \"Customers\": {";
    }

    private static String validIpTablesJson() {
        return "{\n" +
                "    \"Nonce\":0,\n" +
                "    \"Customers\": {\n" +
                "        \"Route53\": {\n" +
                "             \"Mitigation-Groups\" : [\n" +
                "                  {\n" +
                "                      \"Description\":\"Rate Limit TVI Express ANY\",\n" +
                "                      \"Active\": \"true\",\n" +
                "                      \"POPs\": [\"dub2\"],\n" +
                "                      \"Hosts\": [],\n" +
                "                      \"Mitigations\": [\"LIMIT-TVIEXPR-ANY\"]\n" +
                "                  },\n" +
                "\n" +
                "                  {\n" +
                "                      \"Description\":\"Rate Limit TVI Express ANY\",\n" +
                "                      \"Active\": \"true\",\n" +
                "                      \"POPs\": [\"dub3\"],\n" +
                "                      \"Hosts\": [],\n" +
                "                      \"Mitigations\": [\"LIMIT-TVIEXPR-ANY\"]\n" +
                "                  }\n" +
                "\n" +
                "                  ]\n" +
                "        },\n" +
                "        \"CloudFront\": {\n" +
                "        },\n" +
                "        \"CloudFront-DNS\": {\n" +
                "             \"Mitigation-Groups\" : [\n" +
                "                  {\n" +
                "                      \"Description\":\"Rate Limit TVI Express ANY\",\n" +
                "                      \"Active\": \"false\",\n" +
                "                      \"POPs\": [\"sea19\"],\n" +
                "                      \"Hosts\": [],\n" +
                "                      \"Mitigations\": [\"LIMIT-T-SERVICES\"]\n" +
                "                  }\n" +
                "\n" +
                "                  ]\n" +
                "        },\n" +
                "        \"CloudFront-Metro\": {\n" +
                "             \"Mitigation-Groups\" : [\n" +
                "                  {\n" +
                "                      \"Description\":\"Test Mitigation\",\n" +
                "                      \"Active\": \"true\",\n" +
                "                      \"POPs\": [\"SFO5\"],\n" +
                "                      \"Hosts\": [],\n" +
                "                      \"Mitigations\": [\"TCP80COUNT\"]\n" +
                "                  }\n" +
                "\n" +
                "                  ]\n" +
                "        }\n" +
                "\n" +
                "    },\n" +
                "    \"Mitigation-Definitions\": {\n" +
                "        \"LIMIT-TVIEXPR-ANY\": {\n" +
                "             \"Description\": \"Rate Limit tviexpress.com any queries with a zero checksum. Note: ddos-limit is an existing chain that limits to 5k/sec TOTAL\",\n" +
                "             \"Entry-Condition\":\"-p udp --dport 53 -m string --hex-string '|0000|' --algo bm --from 25 --to 26\",\n" +
                "             \"Rules\": [\n" +
                "                 \"-m string --hex-string '|0a7476696578707265737303636f6d0000ff0001|' --algo bm --from 39 --to 40 -j ddos-limit\"\n" +
                "             ]\n" +
                "         },\n" +
                "         \"LIMIT-T-SERVICES\": {\n" +
                "             \"Description\": \"To prevent DNS crash 12/26/2014\",\n" +
                "             \"Entry-Condition\":\"-p tcp --dport 53 -m string --hex-string '|095F7365727669636573|' --algo kmp --from 66 --to 67\",\n" +
                "             \"Rules\": [\n" +
                "                 \"-m string --hex-string '|0A636C6F756466726F6E74036E657400|' --algo kmp --from 66 -j ddos-limit\"\n" +
                "             ]\n" +
                "         },\n" +
                "         \"TCP80COUNT\": {\n" +
                "            \"Description\": \"Drop ALL TCP packets with 0 or 1 SEQ NUM\",\n" +
                "            \"Entry-Condition\":\"-p tcp --dport 80\",\n" +
                "            \"Rules\": [\n" +
                "                \"-m string --hex-string '|00000000|' --algo kmp --from 24 --to 25 -j RETURN\",\n" +
                "                \"-m string --hex-string '|00000001|' --algo kmp --from 24 --to 25 -j RETURN\"\n" +
                "            ]\n" +
                "        }\n" +
                "    },\n" +
                "    \"Custom-Country-Codes\": {\n" +
                "    }\n" +
                "}";
    }
}

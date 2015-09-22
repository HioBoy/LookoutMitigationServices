package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.google.common.collect.ImmutableList;
import com.amazon.lookout.mitigation.service.ApplyIPTablesRulesAction;
import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.Route53SingleCustomerTemplateLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static junitparams.JUnitParamsRunner.$;

@RunWith(JUnitParamsRunner.class)
public class CreateMitigationActivityTest {
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }

    @Test
    public void testIPTablesMitigationRequest() {
        CreateMitigationActivity activity = createActivityWithValidators();
        CreateMitigationRequest request = sampleCreateIPTablesMitigationRequest("TestIPTablesMitigation");

        MitigationModificationResponse response = activity.enact(request);

        assertThat(response.getDeviceName(), is(DeviceName.POP_HOSTS_IP_TABLES.name()));
        assertThat(response.getMitigationName(), is("TestIPTablesMitigation"));
        assertThat(response.getMitigationTemplate(), is(MitigationTemplate.IPTables_Mitigation_EdgeCustomer));
        assertThat(response.getServiceName(), is(ServiceName.Edge));
    }

    @Test
    @Parameters(method = "defaultLocationForIPTablesMitigationRequestParameters")
    public void defaultLocationForIPTablesMitigationRequest(List<String> locations) {
        RequestStorageManager requestStorageManagerMock = mock(RequestStorageManager.class);
        CreateMitigationActivity activity = createActivityWithValidators(requestStorageManagerMock);
        CreateMitigationRequest request = sampleCreateIPTablesMitigationRequest("TestIPTablesMitigation");
        request.setLocations(locations);

        MitigationModificationResponse response = activity.enact(request);

        assertThat(response.getMitigationName(), is("TestIPTablesMitigation"));
        verify(requestStorageManagerMock)
                .storeRequestForWorkflow(
                        eq(request),
                        eq(Sets.newHashSet("EdgeWorldwide")),
                        eq(RequestType.CreateRequest),
                        any(TSDMetrics.class));
    }

    @Test
    public void testBlackholeMitigationRequest() {
        CreateMitigationActivity activity = createActivityWithValidators();
        CreateMitigationRequest request = sampleCreateBlackholeMitigationRequest("TestBlackholeMitigation");

        MitigationModificationResponse response = activity.enact(request);

        assertThat(response.getDeviceName(), is(DeviceName.ARBOR.name()));
        assertThat(response.getMitigationName(), is("TestBlackholeMitigation"));
        assertThat(response.getMitigationTemplate(), is(MitigationTemplate.Blackhole_Mitigation_ArborCustomer));
        assertThat(response.getServiceName(), is(ServiceName.Blackhole));
    }

    @Test
    public void defaultLocationForBlackholeMitigationRequest() {
        RequestStorageManager requestStorageManagerMock = mock(RequestStorageManager.class);
        CreateMitigationActivity activity = createActivityWithValidators(requestStorageManagerMock);
        CreateMitigationRequest request = sampleCreateBlackholeMitigationRequest("TestBlackholeMitigation");

        MitigationModificationResponse response = activity.enact(request);

        assertThat(response.getMitigationName(), is("TestBlackholeMitigation"));
        verify(requestStorageManagerMock)
                .storeRequestForWorkflow(
                        eq(request),
                        eq(Sets.newHashSet("Worldwide")),
                        eq(RequestType.CreateRequest),
                        any(TSDMetrics.class));
    }

    @SuppressWarnings("unused")
    private Object[] defaultLocationForIPTablesMitigationRequestParameters() {
        return $(
                new Object[] { null },
                Lists.newArrayList(),
                Lists.newArrayList((String) null),
                Lists.newArrayList(""),
                Lists.newArrayList("SomeOtherLocationName"),
                Lists.newArrayList("SomeOtherLocationName1", "SomeOtherLocationName2")
        );
    }

    private CreateMitigationActivity createActivityWithValidators() {
        return new CreateMitigationActivity(
            new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class))),
            new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class),
                    mock(EdgeLocationsHelper.class), mock(AmazonS3.class)),
            mock(RequestStorageManager.class),
            mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS),
            new TemplateBasedLocationsManager(mock(Route53SingleCustomerTemplateLocationsHelper.class)));
    }

    private CreateMitigationActivity createActivityWithValidators(RequestStorageManager requestStorageManager) {
        return new CreateMitigationActivity(
            new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class))),
            new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class),
                    mock(EdgeLocationsHelper.class), mock(AmazonS3.class)),
                requestStorageManager,
            mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS),
            new TemplateBasedLocationsManager(mock(Route53SingleCustomerTemplateLocationsHelper.class)));
    }

    private CreateMitigationRequest sampleCreateIPTablesMitigationRequest(String mitigationName) {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setServiceName(ServiceName.Edge);
        request.setMitigationTemplate(MitigationTemplate.IPTables_Mitigation_EdgeCustomer);
        request.setLocations(Lists.newArrayList("EdgeWorldwide"));

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new ApplyIPTablesRulesAction());

        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName("IP_TABLES_RULES");
        constraint.setAttributeValues(ImmutableList.of(getValidIPTablesJson()));
        mitigationDefinition.setConstraint(constraint);

        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }

    private String getValidIPTablesJson() {
        return "{\n" +
                "    \"Nonce\":0,\n" +
                "    \"Customers\": {\n" +
                "        \"Route53\": {\n" +
                "             \"Mitigation-Groups\" : [\n" +
                "                  {\n" +
                "                      \"Description\":\"-Rate Limit TVI Express ANY\",\n" +
                "                      \"Active\": \"true\",\n" +
                "                      \"POPs\": [\"dub2\"],\n" +
                "                      \"Hosts\": [],\n" +
                "                      \"Mitigations\": [\"LIMIT-TVIEXPR-ANY\"]\n" +
                "                  }\n" +
                "\n" +
                "                  ]\n" +
                "        },\n" +
                "        \"CloudFront\": {\n" +
                "        },\n" +
                "        \"CloudFront-DNS\" : {\n" +
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
                "        \"CloudFront-Metro\" : {\n" +
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

    private CreateMitigationRequest sampleCreateBlackholeMitigationRequest(String mitigationName) {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setAction(new DropAction());

        ArborBlackholeConstraint constraint = new ArborBlackholeConstraint();
        constraint.setIp("1.2.3.4/32");
        constraint.setEnabled(true);
        constraint.setTransitProviderIds(emptyList());
        mitigationDefinition.setConstraint(constraint);

        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }
}

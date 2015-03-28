package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.ApplyIPTablesRulesAction;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
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
import com.google.common.collect.Lists;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

public class CreateMitigationActivityTest {
    @BeforeClass
    public static void setup() {
        TestUtils.configure(Level.OFF);
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

    private CreateMitigationActivity createActivityWithValidators() {
        return new CreateMitigationActivity(
            new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class))),
            new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class)),
            mock(RequestStorageManager.class),
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
        mitigationDefinition.setConstraint(new SimpleConstraint());
        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }
}

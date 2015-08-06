package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.tst.TestUtils;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.Route53SingleCustomerTemplateLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;

public class DeleteMitigationFromAllLocationsActivityTest {
    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configure(Level.OFF);
    }

    @Test
    public void enactDeleteNotSupportedForIPTablesEdgeMitigation() throws Exception {
        DeleteMitigationFromAllLocationsActivity activity = createActivityWithValidators();
        DeleteMitigationFromAllLocationsRequest request = sampleDeleteIPTablesMitigationRequest();

        BadRequest400 actualError = assertThrows(BadRequest400.class, () -> activity.enact(request));

        assertThat(actualError.getMessage(), containsString("Delete not supported"));
        assertThat(actualError.getMessage(), containsString(MitigationTemplate.IPTables_Mitigation_EdgeCustomer));
    }

    private DeleteMitigationFromAllLocationsActivity createActivityWithValidators() {
        return new DeleteMitigationFromAllLocationsActivity(
                new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class))),
                new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class),
                        mock(EdgeLocationsHelper.class)),
                mock(RequestStorageManager.class),
                mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS),
                new TemplateBasedLocationsManager(mock(Route53SingleCustomerTemplateLocationsHelper.class)));
    }

    private DeleteMitigationFromAllLocationsRequest sampleDeleteIPTablesMitigationRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("IPTablesMitigationName");
        request.setServiceName(ServiceName.Edge);
        request.setMitigationTemplate(MitigationTemplate.IPTables_Mitigation_EdgeCustomer);
        request.setMitigationVersion(1);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        return request;
    }
}

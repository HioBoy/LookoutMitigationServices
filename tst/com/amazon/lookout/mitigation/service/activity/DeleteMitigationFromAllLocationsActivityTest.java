package com.amazon.lookout.mitigation.service.activity;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.amazon.lookout.workflow.helper.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.BlackWatchTemplateLocationHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.Route53SingleCustomerTemplateLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

public class DeleteMitigationFromAllLocationsActivityTest {

    private static final int MITIGATION_VERSION = 1;

    @BeforeClass
    public static void setUpOnce() {
        TestUtils.configureLogging();
    }

    @Test
    public void enactDeleteNotSupportedForIPTablesEdgeMitigation() throws Exception {
        DeleteMitigationFromAllLocationsActivity activity = createActivityWithValidators();
        DeleteMitigationFromAllLocationsRequest request = sampleDeleteIPTablesMitigationRequest();

        BadRequest400 actualError = assertThrows(BadRequest400.class, () -> activity.enact(request));

        assertThat(actualError.getMessage(), containsString("not supported"));
        assertThat(actualError.getMessage(), containsString(MitigationTemplate.IPTables_Mitigation_EdgeCustomer));
    }

    private DeleteMitigationFromAllLocationsActivity createActivityWithValidators(SWFWorkflowStarter workflowStarter) {
        RequestStorageManager requestStorageManager = mock(RequestStorageManager.class);
        Mockito.doReturn(new RequestStorageResponse(1l, MITIGATION_VERSION)).when(requestStorageManager).storeRequestForWorkflow(
                any(MitigationModificationRequest.class), anySet(), eq(RequestType.DeleteRequest), isA(TSDMetrics.class));
        return new DeleteMitigationFromAllLocationsActivity(
            new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class)),
                    mock(EdgeLocationsHelper.class),
                    mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class)),
            new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class),
                    mock(EdgeLocationsHelper.class), mock(AmazonS3.class),
                    mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class)),
            requestStorageManager,
            workflowStarter,
            new TemplateBasedLocationsManager(mock(Route53SingleCustomerTemplateLocationsHelper.class),
                    mock(BlackWatchTemplateLocationHelper.class)));
    }

    private DeleteMitigationFromAllLocationsActivity createActivityWithValidators() {
        return createActivityWithValidators(mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS));
    }

    private DeleteMitigationFromAllLocationsRequest sampleDeleteIPTablesMitigationRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("IPTablesMitigationName");
        request.setServiceName(ServiceName.Edge);
        request.setMitigationTemplate(MitigationTemplate.IPTables_Mitigation_EdgeCustomer);
        request.setMitigationVersion(MITIGATION_VERSION);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        return request;
    }
}

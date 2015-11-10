package com.amazon.lookout.mitigation.service.activity;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.BlackWatchTemplateLocationHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.Route53SingleCustomerTemplateLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.mitigation.workers.helper.BlackholeMitigationHelper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.simpleworkflow.flow.WorkflowClientExternal;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.amazon.lookout.test.common.util.AssertUtils.assertThrows;
import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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

        assertThat(actualError.getMessage(), containsString("Delete not supported"));
        assertThat(actualError.getMessage(), containsString(MitigationTemplate.IPTables_Mitigation_EdgeCustomer));
    }

    @Test
    public void enactStartsWorkflowForBlackholeMitigation() {
        SWFWorkflowStarter workflowStarterMock = mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS);
        DeleteMitigationFromAllLocationsActivity activity = createActivityWithValidators(workflowStarterMock);
        DeleteMitigationFromAllLocationsRequest request = sampleDeleteBlackholeMitigationRequest();

        activity.enact(request);

        verify(workflowStarterMock).startMitigationModificationWorkflow(
            anyLong(),
            eq(request),
            eq(newHashSet(StandardLocations.ARBOR)),
            eq(RequestType.DeleteRequest),
            eq(MITIGATION_VERSION),
            eq(DeviceName.ARBOR.name()),
            eq(DeviceScope.GLOBAL.name()),
            any(WorkflowClientExternal.class),
            any(TSDMetrics.class));
    }

    private DeleteMitigationFromAllLocationsActivity createActivityWithValidators(SWFWorkflowStarter workflowStarter) {
        return new DeleteMitigationFromAllLocationsActivity(
            new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class))),
            new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class),
                    mock(EdgeLocationsHelper.class), mock(AmazonS3.class), mock(BlackholeMitigationHelper.class),
                    mock(BlackWatchBorderLocationValidator.class)),
            mock(RequestStorageManager.class),
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

    private DeleteMitigationFromAllLocationsRequest sampleDeleteBlackholeMitigationRequest() {
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("LKT-TestBlackholeMitigation");
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
        request.setMitigationVersion(MITIGATION_VERSION);

        MitigationActionMetadata actionMetadata = new MitigationActionMetadata();
        actionMetadata.setUser("username");
        actionMetadata.setToolName("unit-tests");
        actionMetadata.setDescription("description");
        request.setMitigationActionMetadata(actionMetadata);

        return request;
    }
}

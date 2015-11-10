package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.ArborBlackholeConstraint;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceSubnetsMatcher;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.BlackWatchTemplateLocationHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.Route53SingleCustomerTemplateLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Sets;

import junitparams.JUnitParamsRunner;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(JUnitParamsRunner.class)
public class EditMitigationActivityTest {
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }

    @Test
    public void testBlackholeMitigationRequest() {
        EditMitigationActivity activity = createActivityWithValidators();
        EditMitigationRequest request = sampleEditBlackholeMitigationRequest("LKT-TestBlackholeMitigation");

        MitigationModificationResponse response = activity.enact(request);

        assertThat(response.getDeviceName(), is(DeviceName.ARBOR.name()));
        assertThat(response.getMitigationName(), is("LKT-TestBlackholeMitigation"));
        assertThat(response.getMitigationTemplate(), is(MitigationTemplate.Blackhole_Mitigation_ArborCustomer));
        assertThat(response.getServiceName(), is(ServiceName.Blackhole));
    }

    @Test
    public void defaultLocationForBlackholeMitigationRequest() {
        RequestStorageManager requestStorageManagerMock = mock(RequestStorageManager.class);
        EditMitigationActivity activity = createActivityWithValidators(requestStorageManagerMock);
        EditMitigationRequest request = sampleEditBlackholeMitigationRequest("LKT-TestBlackholeMitigation");

        MitigationModificationResponse response = activity.enact(request);

        assertThat(response.getMitigationName(), is("LKT-TestBlackholeMitigation"));
        verify(requestStorageManagerMock)
                .storeRequestForWorkflow(
                    eq(request),
                    eq(Sets.newHashSet(StandardLocations.ARBOR)),
                    eq(RequestType.EditRequest),
                    any(TSDMetrics.class));
    }

    private EditMitigationActivity createActivityWithValidators() {
        return createActivityWithValidators(mock(RequestStorageManager.class));
    }

    private EditMitigationActivity createActivityWithValidators(RequestStorageManager requestStorageManager) {
        return new EditMitigationActivity(
            new RequestValidator(new ServiceLocationsHelper(mock(EdgeLocationsHelper.class))),
            new TemplateBasedRequestValidator(mock(ServiceSubnetsMatcher.class),
                    mock(EdgeLocationsHelper.class), mock(AmazonS3.class), BlackholeTestUtils.mockMitigationHelper(),
                    mock(BlackWatchBorderLocationValidator.class)),
                requestStorageManager,
            mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS),
            new TemplateBasedLocationsManager(mock(Route53SingleCustomerTemplateLocationsHelper.class),
                    mock(BlackWatchTemplateLocationHelper.class)));
    }

    private EditMitigationRequest sampleEditBlackholeMitigationRequest(String mitigationName) {
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName(mitigationName);
        request.setServiceName(ServiceName.Blackhole);
        request.setMitigationTemplate(MitigationTemplate.Blackhole_Mitigation_ArborCustomer);
        request.setMitigationVersion(2);

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
        constraint.setTransitProviderIds(Collections.singletonList(BlackholeTestUtils.VALID_SUPPORTED_TRANSIT_PROVIDER_ID_16509_1));
        mitigationDefinition.setConstraint(constraint);

        request.setMitigationDefinition(mitigationDefinition);
        return request;
    }
}

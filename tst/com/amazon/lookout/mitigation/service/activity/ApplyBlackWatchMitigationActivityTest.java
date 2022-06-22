package com.amazon.lookout.mitigation.service.activity;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

import java.util.Arrays;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig.GlobalDeployment;
import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.ApplyBlackWatchMitigationResponse;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;

@RunWith(JUnitParamsRunner.class)
public class ApplyBlackWatchMitigationActivityTest extends ActivityTestHelper {
    private ApplyBlackWatchMitigationRequest request;

    private ApplyBlackWatchMitigationActivity applyBlackWatchMitigationActivity;

    @Before
    public void setup() {
        TestUtils.configureLogging();
        applyBlackWatchMitigationActivity = 
                spy(new ApplyBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler));
        identity.setAttribute(Identity.AWS_USER_ARN, userArn);
        request = new ApplyBlackWatchMitigationRequest();
        request.setResourceId("AddressList1234");
        request.setResourceType("IPAddressList");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());        
        when(requestValidator.validateApplyBlackWatchMitigationRequest(request, userArn)).thenReturn(new BlackWatchTargetConfig());

        Mockito.doReturn(requestId).when(applyBlackWatchMitigationActivity).getRequestId();
        Mockito.doReturn(identity).when(applyBlackWatchMitigationActivity).getIdentity();
    }

    @Test
    public void testApplyBlackWatchMitigationActivity() {
        //applyBWMitigation Doesn't do much, so we only test the functionality that the activity directly adds.
        //The other areas are tested with the handler.
        ApplyBlackWatchMitigationResponse retResponse = new ApplyBlackWatchMitigationResponse();
        Mockito.doReturn(retResponse).when(blackwatchMitigationInfoHandler).applyBlackWatchMitigation(anyString(), 
                anyString(), anyInt(),  isA(MitigationActionMetadata.class),
                isA(BlackWatchTargetConfig.class), anyString(), isA(TSDMetrics.class), anyBoolean(), anyBoolean());

        ApplyBlackWatchMitigationResponse response = applyBlackWatchMitigationActivity.enact(request);
        assertEquals(requestId, response.getRequestId());
    }

    @Test(expected = BadRequest400.class)
    @Parameters({
            "GLOBAL",
            "REGIONAL",
            "GLOBAL REGIONAL"})
    public void whenPlacementTagsAreSuppliedThenFail(String placementTags) {
        // placement_tags aren't supported yet https://issues.amazon.com/issues/BLACKWATCH-7739
        GlobalDeployment globalDeployment = new GlobalDeployment();
        globalDeployment.setPlacement_tags(Arrays.stream(placementTags.split(" ")).collect(toSet()));
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        targetConfig.setGlobal_deployment(globalDeployment);
        when(requestValidator.validateApplyBlackWatchMitigationRequest(request, userArn)).thenReturn(targetConfig);

        applyBlackWatchMitigationActivity.enact(request);
    }
}

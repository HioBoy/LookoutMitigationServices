package com.amazon.lookout.mitigation.service.activity;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

import java.util.Arrays;

import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig.GlobalDeployment;
import com.amazon.blackwatch.mitigation.state.model.MitigationState;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.mitigation.state.model.BlackWatchTargetConfig;
import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationResponse;

@RunWith(JUnitParamsRunner.class)
public class UpdateBlackWatchMitigationActivityTest extends ActivityTestHelper {
    private UpdateBlackWatchMitigationRequest request;

    private UpdateBlackWatchMitigationActivity updateBlackWatchMitigationActivity;

    @Before
    public void setup() {
        TestUtils.configureLogging();
        updateBlackWatchMitigationActivity = 
                spy(new UpdateBlackWatchMitigationActivity(requestValidator, blackwatchMitigationInfoHandler, false));
        identity.setAttribute(Identity.AWS_USER_ARN, userArn);
        request = new UpdateBlackWatchMitigationRequest();
        request.setMitigationId("");
        request.setMitigationActionMetadata(
                MitigationActionMetadata.builder()
                .withUser("Khaleesi")
                .withToolName("JUnit")
                .withDescription("Test Descr")
                .withRelatedTickets(Arrays.asList("1234", "5655"))
                .build());

        when(requestValidator.validateUpdateBlackWatchMitigationRequest(
                    isA(UpdateBlackWatchMitigationRequest.class),
                    anyString(),
                    isA(BlackWatchTargetConfig.class),
                    anyString())).thenReturn(new BlackWatchTargetConfig());

        Mockito.doReturn(requestId).when(updateBlackWatchMitigationActivity).getRequestId();
        Mockito.doReturn(identity).when(updateBlackWatchMitigationActivity).getIdentity();
    }

    @Test
    public void testUpdateBlackWatchMitigationActivity() {
        //updateBWMitigation Doesn't do much, so we only test the functionality that the activity directly adds.
        //The other areas are tested with the handler.
        UpdateBlackWatchMitigationResponse retResponse = new UpdateBlackWatchMitigationResponse();
        MitigationState mitigationState = MitigationState.builder()
                .mitigationId("123")
                .resourceType("IPAddress")
                .build();
        Mockito.doReturn(mitigationState).when(blackwatchMitigationInfoHandler).getMitigationState(anyString());
        Mockito.doReturn(retResponse).when(blackwatchMitigationInfoHandler).updateBlackWatchMitigation(anyString(), 
                anyInt(), isA(MitigationActionMetadata.class), isA(BlackWatchTargetConfig.class),
                anyString(), isA(TSDMetrics.class), anyBoolean());
        
        UpdateBlackWatchMitigationResponse response = updateBlackWatchMitigationActivity.enact(request);
        assertEquals(requestId, response.getRequestId());
    }

    @Test(expected = BadRequest400.class)
    @Parameters({
            "GLOBAL",
            "REGIONAL",
            "GLOBAL REGIONAL"})
    public void whenPlacementTagsAreSuppliedThenFail(String placementTags) {
        // placement_tags aren't supported yet https://issues.amazon.com/issues/BLACKWATCH-7739
        MitigationState mitigationState = MitigationState.builder()
                .mitigationId("123")
                .resourceType("IPAddress")
                .build();
        Mockito.doReturn(mitigationState).when(blackwatchMitigationInfoHandler).getMitigationState(anyString());

        GlobalDeployment globalDeployment = new GlobalDeployment();
        globalDeployment.setPlacement_tags(Arrays.stream(placementTags.split(" ")).collect(toSet()));
        BlackWatchTargetConfig targetConfig = new BlackWatchTargetConfig();
        targetConfig.setGlobal_deployment(globalDeployment);
        when(requestValidator.validateUpdateBlackWatchMitigationRequest(
                isA(UpdateBlackWatchMitigationRequest.class),
                anyString(),
                isA(BlackWatchTargetConfig.class),
                anyString())).thenReturn(targetConfig);

        updateBlackWatchMitigationActivity.enact(request);
    }
}

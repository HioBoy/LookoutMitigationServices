package com.amazon.lookout.mitigation.service.activity;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazon.lookout.mitigation.service.BadRequest400;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementRequest;
import com.amazon.lookout.mitigation.service.UpdateBlackWatchMitigationRegionalCellPlacementResponse;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.aws158.commons.metric.TSDMetrics;


@RunWith(JUnitParamsRunner.class)
public class UpdateBlackWatchMitigationRegionalCellPlacementActivityTest extends ActivityTestHelper {
    private UpdateBlackWatchMitigationRegionalCellPlacementRequest request;

    private UpdateBlackWatchMitigationRegionalCellPlacementActivity
            updateBlackWatchMitigationRegionalCellPlacementActivity;

    private final String mitigationId = "REGIONAL-MITIGATION";

    @Before
    public void setup() {
        TestUtils.configureLogging();
        updateBlackWatchMitigationRegionalCellPlacementActivity =
                spy(new UpdateBlackWatchMitigationRegionalCellPlacementActivity(requestValidator, blackwatchMitigationInfoHandler));
        request = new UpdateBlackWatchMitigationRegionalCellPlacementRequest();

        request.setMitigationId(mitigationId);

        Mockito.doReturn(requestId).when(updateBlackWatchMitigationRegionalCellPlacementActivity).getRequestId();
    }

    @Test
    public void testUpdateBlackWatchMitigationRegionalCellPlacementActivityWithValidCellNames() {
        UpdateBlackWatchMitigationRegionalCellPlacementResponse retResponse =
                new UpdateBlackWatchMitigationRegionalCellPlacementResponse();

        List<String> cellNames = Stream.of("bzg-pdx-c1", "bz-pdx-c2").collect(Collectors.toList());
        request.setCellNames(cellNames);

        Mockito.doReturn(retResponse).when(blackwatchMitigationInfoHandler)
                .updateBlackWatchMitigationRegionalCellPlacement(anyString(), anyListOf(String.class),
                        anyString(), isA(TSDMetrics.class));

        UpdateBlackWatchMitigationRegionalCellPlacementResponse response =
                updateBlackWatchMitigationRegionalCellPlacementActivity.enact(request);
        assertEquals(requestId, response.getRequestId());
    }

    @Test(expected = BadRequest400.class)
    public void testUpdateBlackWatchMitigationRegionalCellPlacementActivityWhenRequestValidationFails() {
        doThrow(new IllegalArgumentException()).when(requestValidator)
                .validateUpdateBlackWatchMitigationRegionalCellPlacementRequest(
                        isA(UpdateBlackWatchMitigationRegionalCellPlacementRequest.class)
                );

        updateBlackWatchMitigationRegionalCellPlacementActivity.enact(request);
    }

    @Test(expected = InternalServerError500.class)
    public void testUpdateBlackWatchMitigationRegionalCellPlacementActivityWhenIllegalStateExceptionThrownInternally() {
        doThrow(IllegalStateException.class).when(blackwatchMitigationInfoHandler)
                .updateBlackWatchMitigationRegionalCellPlacement(anyString(), anyListOf(String.class),
                anyString(), isA(TSDMetrics.class));

        updateBlackWatchMitigationRegionalCellPlacementActivity.enact(request);
    }

    @Test(expected = InternalServerError500.class)
    public void testUpdateBlackWatchMitigationRegionalCellPlacementActivityWhenConditionalCheckFailedExceptionThrownInternally() {
        doThrow(ConditionalCheckFailedException.class).when(blackwatchMitigationInfoHandler)
                .updateBlackWatchMitigationRegionalCellPlacement(anyString(), anyListOf(String.class),
                        anyString(), isA(TSDMetrics.class));

        updateBlackWatchMitigationRegionalCellPlacementActivity.enact(request);
    }
}

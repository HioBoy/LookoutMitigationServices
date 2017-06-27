package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationResponse;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.amazon.lookout.workflow.helper.SWFWorkflowStarter;
import com.amazon.lookout.mitigation.service.workflow.helper.BlackWatchTemplateLocationHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.TemplateBasedLocationsManager;
import com.amazon.lookout.model.RequestType;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Sets;

import junitparams.JUnitParamsRunner;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(JUnitParamsRunner.class)
public class EditMitigationActivityTest {
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }

    private EditMitigationActivity createActivityWithValidators() {
        return createActivityWithValidators(mock(RequestStorageManager.class));
    }

    private EditMitigationActivity createActivityWithValidators(RequestStorageManager requestStorageManager) {
        Mockito.doReturn(new RequestStorageResponse(1, 1)).when(requestStorageManager)
                .storeRequestForWorkflow(any(MitigationModificationRequest.class), anySet(), 
                        eq(RequestType.EditRequest), isA(TSDMetrics.class));
        return new EditMitigationActivity(
            new RequestValidator(mock(EdgeLocationsHelper.class),
                    mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class)),
            new TemplateBasedRequestValidator(mock(EdgeLocationsHelper.class), mock(AmazonS3.class),
                    mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class)),
                requestStorageManager,
            mock(SWFWorkflowStarter.class, RETURNS_DEEP_STUBS),
            new TemplateBasedLocationsManager(mock(BlackWatchTemplateLocationHelper.class)));
    }
}

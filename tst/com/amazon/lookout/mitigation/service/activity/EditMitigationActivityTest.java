package com.amazon.lookout.mitigation.service.activity;

import java.util.Collections;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.services.s3.AmazonS3;

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

import com.amazon.lookout.mitigation.RequestCreator;

@RunWith(JUnitParamsRunner.class)
public class EditMitigationActivityTest {
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }

    private EditMitigationActivity createActivityWithValidators() {
        return new EditMitigationActivity(
            new RequestValidator(mock(EdgeLocationsHelper.class),
                    mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class),
                    "/random/path/location/json"),
            new TemplateBasedRequestValidator(mock(EdgeLocationsHelper.class), mock(AmazonS3.class),
                    mock(BlackWatchBorderLocationValidator.class),
                    mock(BlackWatchEdgeLocationValidator.class)),
            mock(RequestCreator.class));
    }
}


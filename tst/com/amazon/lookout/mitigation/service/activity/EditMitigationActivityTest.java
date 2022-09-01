package com.amazon.lookout.mitigation.service.activity;


import com.amazon.blackwatch.bwircellconfig.model.BwirCellConfig;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.test.common.util.TestUtils;

import junitparams.JUnitParamsRunner;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;

import com.amazon.lookout.mitigation.RequestCreator;

@RunWith(JUnitParamsRunner.class)
public class EditMitigationActivityTest {
    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }

    private EditMitigationActivity createActivityWithValidators() {
        return new EditMitigationActivity(
            new RequestValidator("/random/path/location/json",
                    mock(BwirCellConfig.class)),
            new TemplateBasedRequestValidator(),
            mock(RequestCreator.class));
    }
}


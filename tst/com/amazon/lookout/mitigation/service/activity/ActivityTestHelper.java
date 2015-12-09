package com.amazon.lookout.mitigation.service.activity;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.BeforeClass;

import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.test.common.util.TestUtils;

public class ActivityTestHelper {
    protected static final String serviceName = ServiceName.AWS;
    protected static final String deviceName = DeviceName.BLACKWATCH_BORDER.name();
    protected static final String deviceScope = DeviceScope.GLOBAL.name();
    protected static final String mitigationName = "mitigation1";
    protected static final int rollbackMitigationVersion = 100;
    protected static final int mitigationVersion = 200;
    protected static final String mitigationTemplate = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    protected static final int exclusiveStartVersion = 10;
    protected static final long workflowId = 1000;
    protected static final int maxNumberOfHistoryEntriesToFetch = 20;
    protected static final String requestId = "1000001";
    protected static final List<String> locations = Arrays.asList("G-IAD55", "G-SFO5");

    protected static RequestValidator requestValidator;
    protected static RequestInfoHandler requestInfoHandler;
    protected static MitigationInstanceInfoHandler mitigationInstanceInfoHandler;

    @BeforeClass
    public static void setupOnce() {
        TestUtils.configureLogging();
    }

    @Before
    public void resetMock() {
        requestValidator = mock(RequestValidator.class);
        requestInfoHandler = mock(RequestInfoHandler.class);
        mitigationInstanceInfoHandler = mock(MitigationInstanceInfoHandler.class);
    }
}
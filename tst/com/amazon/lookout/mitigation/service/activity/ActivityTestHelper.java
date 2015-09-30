package com.amazon.lookout.mitigation.service.activity;

import org.junit.Before;
import org.junit.BeforeClass;

import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.activity.helper.MitigationInstanceInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.test.common.util.TestUtils;

public class ActivityTestHelper {
    protected static final String serviceName = ServiceName.AWS;
    protected static final String deviceName = DeviceName.BLACKWATCH_BORDER.name();
    protected static final String deviceScope = DeviceScope.GLOBAL.name();
    protected static final String mitigationName = "mitigation1";
    protected static final int exclusiveStartVersion = 10;
    protected static final int maxNumberOfHistoryEntriesToFetch = 20;
    protected static final String requestId = "1000001";

    protected static RequestValidator requestValidator;
    protected static RequestInfoHandler requestInfoHandler;
    protected static MitigationInstanceInfoHandler mitigationInstanceInfoHandler;

    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }

    @Before
    public void resetMock() {
        requestValidator = mock(RequestValidator.class);
        requestInfoHandler = mock(RequestInfoHandler.class);
        mitigationInstanceInfoHandler = mock(MitigationInstanceInfoHandler.class);
    }
}

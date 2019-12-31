package com.amazon.lookout.mitigation.service.activity;

import java.util.Arrays;
import java.util.List;

import com.amazon.blackwatch.location.state.storage.LocationStateDynamoDBHelper;
import com.amazon.lookout.mitigation.datastore.LocationStateLocksDAO;
import com.amazon.lookout.mitigation.service.activity.helper.mws.MWSHelper;
import com.amazon.lookout.mitigation.service.activity.validator.HostnameValidator;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.mockito.Mockito.*;

import com.amazon.coral.service.Identity;
import com.amazon.lookout.mitigation.ActiveMitigationsHelper;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.HostStatusInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.test.common.util.TestUtils;

public class ActivityTestHelper {
    protected static final String deviceName = DeviceName.BLACKWATCH_BORDER.name();
    protected static final String mitigationName = "mitigation1";
    protected static final int rollbackMitigationVersion = 100;
    protected static final int mitigationVersion = 200;
    protected static final String mitigationTemplate = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    protected static final long workflowId = 1000;
    protected static final int maxNumberOfHistoryEntriesToFetch = 20;
    protected static final String requestId = "1000001";
    protected static final Identity identity = new Identity();
    protected static final List<String> locations = Arrays.asList("G-IAD55");
    protected static final MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
    protected static final String userArn = "arn:12324554";

    static {
        mitigationActionMetadata.setDescription("desc");
        mitigationActionMetadata.setUser("nobody");
        mitigationActionMetadata.setToolName("CLI");
    }

    protected static HostnameValidator hostnameValidator;
    protected static RequestValidator requestValidator;
    protected static HostStatusInfoHandler hostStatusInfoHandler;
    protected static LocationStateInfoHandler locationStateInfoHandler;
    protected static BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;
    protected static ActiveMitigationsHelper activeMitigationsHelper;
    protected static MWSHelper mwsHelper;
    protected static LocationStateDynamoDBHelper locationStateDynamoDBHelper;
    protected static LocationStateLocksDAO locationStateLocksDAO;

    @BeforeClass
    public static void setupOnce() {
        TestUtils.configureLogging();
    }

    @Before
    public void resetMock() {
        hostnameValidator = mock(HostnameValidator.class);
        requestValidator = mock(RequestValidator.class);
        hostStatusInfoHandler = mock(HostStatusInfoHandler.class);
        locationStateInfoHandler = mock(LocationStateInfoHandler.class);
        blackwatchMitigationInfoHandler = mock(BlackWatchMitigationInfoHandler.class);
        activeMitigationsHelper = mock(ActiveMitigationsHelper.class);
        locationStateLocksDAO = mock(LocationStateLocksDAO.class);
        locationStateDynamoDBHelper = mock(LocationStateDynamoDBHelper.class);
        mwsHelper = mock(MWSHelper.class);
    }
}

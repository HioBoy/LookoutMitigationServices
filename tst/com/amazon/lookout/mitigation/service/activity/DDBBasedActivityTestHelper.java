package com.amazon.lookout.mitigation.service.activity;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchELBResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchIPAddressListResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchIPAddressResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.BlackWatchResourceTypeHelper;
import com.amazon.blackwatch.mitigation.resource.helper.ELBResourceHelper;
import com.amazon.blackwatch.mitigation.state.storage.MitigationStateDynamoDBHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationHelper;
import com.amazon.blackwatch.mitigation.state.storage.ResourceAllocationStateDynamoDBHelper;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.service.Activity;
import com.amazon.coral.service.Context;
import com.amazon.coral.service.Identity;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchMitigationResourceType;
import com.amazon.blackwatch.mitigation.resource.validator.BlackWatchResourceTypeValidator;
import com.amazon.blackwatch.mitigation.resource.validator.IPAddressListResourceTypeValidator;
import com.amazon.blackwatch.mitigation.resource.validator.IPAddressResourceTypeValidator;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.activity.helper.BlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.helper.dynamodb.DDBBasedBlackWatchMitigationInfoHandler;
import com.amazon.lookout.mitigation.service.activity.validator.RequestValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchBorderLocationValidator;
import com.amazon.lookout.mitigation.service.activity.validator.template.BlackWatchEdgeLocationValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.DogFishValidationHelper;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.google.common.collect.ImmutableMap;

public class DDBBasedActivityTestHelper {
    protected static final String serviceName = ServiceName.AWS;
    protected static final String deviceName = DeviceName.BLACKWATCH_BORDER.name();
    protected static final String mitigationName = "mitigation1";
    protected static final int rollbackMitigationVersion = 100;
    protected static final int mitigationVersion = 200;
    protected static final String mitigationTemplate = MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer;
    protected static final int exclusiveStartVersion = 10;
    protected static final long workflowId = 1000;
    protected static final int maxNumberOfHistoryEntriesToFetch = 20;
    protected static final String requestId = "1000001";
    
    protected static final String userArn = "arn:12324554";
    protected static final Identity identity = new Identity();
    static {
        identity.setAttribute(Identity.AWS_USER_ARN, userArn);
    }
    
    protected static final List<String> locations = Arrays.asList("G-IAD55", "G-SFO5");
    protected static final RequestStorageResponse requestStorageResponse = new RequestStorageResponse(workflowId, mitigationVersion);
    protected static final MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
    protected static final String realm = "us-east-1";
    protected static final String domain = "test";
    protected static final int parallelScanSegments = 3;
    
    static {
        mitigationActionMetadata.setDescription("desc");
        mitigationActionMetadata.setUser("nobody");
        mitigationActionMetadata.setToolName("CLI");
    }
    
    protected static AmazonDynamoDBClient dynamoDBClient;
    
    protected static MetricsFactory metricsFactory = Mockito.mock(MetricsFactory.class);
    protected static Metrics metrics = Mockito.mock(Metrics.class);

    protected static final long readCapacityUnits = 5L;
    protected static final long writeCapacityUnits = 5L;
    protected static MitigationStateDynamoDBHelper mitigationStateDDBHelper;
    protected static ResourceAllocationStateDynamoDBHelper resourceAllocationStateDDBHelper;
    protected static ResourceAllocationHelper resourceAllocationHelper;
    protected static Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeValidator> resourceTypeValidatorMap;

    protected EdgeLocationsHelper edgeLocationsHelper;
    protected BlackWatchBorderLocationValidator blackWatchBorderLocationValidator;
    protected BlackWatchEdgeLocationValidator blackWatchEdgeLocationValidator;
    protected RequestValidator requestValidator;
    private static ELBResourceHelper elbResourceHelper;
    private static Map<BlackWatchMitigationResourceType, BlackWatchResourceTypeHelper> resourceTypeHelpers;

    protected DogFishValidationHelper dogfishHelper;
    protected BlackWatchMitigationInfoHandler blackwatchMitigationInfoHandler;
    
    @BeforeClass
    public static void setupOnce() {
        TestUtils.configureLogging(Level.ERROR);

        // connect to DynamoDB local
        AWSCredentials credentials = new BasicAWSCredentials("test", "");
        dynamoDBClient = new AmazonDynamoDBClient(credentials);
        String endpoint = System.getProperty("dynamodb-local.endpoint");
        dynamoDBClient.setEndpoint(endpoint);
        
        // mock Metrics Factory
        doReturn(metrics).when(metricsFactory).newMetrics();
        doReturn(metrics).when(metrics).newMetrics();

        mitigationStateDDBHelper = new MitigationStateDynamoDBHelper(
                dynamoDBClient, realm, domain, readCapacityUnits, writeCapacityUnits, metricsFactory);
        resourceAllocationStateDDBHelper = new ResourceAllocationStateDynamoDBHelper(
                dynamoDBClient, realm, domain, readCapacityUnits, writeCapacityUnits, metricsFactory);
        resourceAllocationHelper = new ResourceAllocationHelper(
                mitigationStateDDBHelper, resourceAllocationStateDDBHelper, metricsFactory);
        
        elbResourceHelper = Mockito.mock(ELBResourceHelper.class);
        
        BlackWatchELBResourceTypeHelper elbResourceTypeHelper =  new BlackWatchELBResourceTypeHelper(elbResourceHelper, metricsFactory);
        BlackWatchIPAddressResourceTypeHelper ipadressResourceTypeHelper =  new BlackWatchIPAddressResourceTypeHelper();
        BlackWatchIPAddressListResourceTypeHelper ipaddressListResourceTypeHelper =  new BlackWatchIPAddressListResourceTypeHelper();
        resourceTypeHelpers = ImmutableMap.of(
                BlackWatchMitigationResourceType.IPAddress, ipadressResourceTypeHelper,
                BlackWatchMitigationResourceType.IPAddressList, ipaddressListResourceTypeHelper,
                BlackWatchMitigationResourceType.ELB, elbResourceTypeHelper
                );
        resourceTypeValidatorMap = new EnumMap<>(BlackWatchMitigationResourceType.class);
        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.IPAddress, new IPAddressResourceTypeValidator());
        resourceTypeValidatorMap.put(BlackWatchMitigationResourceType.IPAddressList, new IPAddressListResourceTypeValidator());
    }

    @Before
    public void resetState() {
        mitigationStateDDBHelper.deleteTable();
        mitigationStateDDBHelper.createTableIfNotExist(readCapacityUnits, writeCapacityUnits);
        
        resourceAllocationStateDDBHelper.deleteTable();
        resourceAllocationStateDDBHelper.createTableIfNotExist(readCapacityUnits, writeCapacityUnits);
        
        edgeLocationsHelper = mock(EdgeLocationsHelper.class);
        blackWatchBorderLocationValidator = mock(BlackWatchBorderLocationValidator.class);
        blackWatchEdgeLocationValidator = mock(BlackWatchEdgeLocationValidator.class);
        requestValidator = new RequestValidator(edgeLocationsHelper,
                blackWatchBorderLocationValidator, blackWatchEdgeLocationValidator);
        

        dogfishHelper = mock(DogFishValidationHelper.class);
        blackwatchMitigationInfoHandler = new DDBBasedBlackWatchMitigationInfoHandler(mitigationStateDDBHelper,
                resourceAllocationStateDDBHelper, resourceAllocationHelper, dogfishHelper, resourceTypeValidatorMap,
                resourceTypeHelpers, parallelScanSegments, realm);
    }
    
    protected <T extends Activity> T setupActivity(T activity) {
        Context context = mock(Context.class);
        when(context.getMetrics()).thenReturn(metrics);
        when(context.getIdentity()).thenReturn(identity);
        when(context.getRequestId()).thenReturn(requestId);
        when(context.getRequestDate()).thenReturn(System.currentTimeMillis());
        activity.setContext(context);
        return activity;
    }
}

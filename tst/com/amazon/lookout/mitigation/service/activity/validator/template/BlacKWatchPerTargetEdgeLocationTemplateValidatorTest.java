package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.mitigation.service.Alarm;
import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.S3Object;
import com.amazon.lookout.mitigation.service.alarm.AlarmType;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazonaws.services.s3.AmazonS3;

public class BlacKWatchPerTargetEdgeLocationTemplateValidatorTest {
       
    @Mock
    private EdgeLocationsHelper edgeLocationsHelper;
    
    @Mock
    private MetricsFactory metricsFactory;
    
    @Mock
    protected Metrics metrics;
    
    @Mock
    protected TSDMetrics tsdMetric;
    
    @Mock
    protected AmazonS3 s3Client;
    
    private BlackWatchPerTargetEdgeLocationTemplateValidator validator;
    
    private static final AlarmCheck ALARM_CHECK = new AlarmCheck(); 
    static {
        ALARM_CHECK.setDelaySec(0);
        ALARM_CHECK.setCheckEveryNSec(60);
        ALARM_CHECK.setCheckTotalPeriodSec(600);
        Map<String, List<Alarm>> alarms = new HashMap<>();
        Alarm alarm = new Alarm();
        alarm.setName("fake.bw.carnaval.alarm");
        alarms.put(AlarmType.CARNAVAL, Arrays.asList(alarm));
        ALARM_CHECK.setAlarms(alarms);
    }
    
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        // mock TSDMetric
        doReturn(metrics).when(metricsFactory).newMetrics();
        doReturn(metrics).when(metrics).newMetrics();
        validator = new BlackWatchPerTargetEdgeLocationTemplateValidator(new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)$", new HashSet<String>(Arrays.asList("LOADTEST-118", "LOADTEST-119")), ""), s3Client);
        
        doReturn(new HashSet<String>(Arrays.asList("AMS1", "AMS50", "NRT54", "G-IAD55")))
                .when(edgeLocationsHelper).getAllClassicPOPs();
    }

    /**
     * Test location validator validate location match pattern
     */
    @Test
    public void testLocationValidator() {
        try{
            String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
            CreateMitigationRequest request = new CreateMitigationRequest();
            request.setMitigationName("BLACKWATCH_POP_GLOBAL_G-IAD55");
            request.setMitigationTemplate(mitigationTemplate);
            request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
            request.setServiceName(ServiceName.Edge);
            request.setLocations(Arrays.asList("g-iad55"));
            S3Object config = new S3Object();
            config.setBucket("s3bucket");
            config.setKey("s3key");
            config.setMd5("md5");
            BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
            constraint.setConfig(config);
            MitigationDefinition mitigationDefinition = new MitigationDefinition();
            mitigationDefinition.setConstraint(constraint);
            request.setMitigationDefinition(mitigationDefinition);
            validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
            Assert.fail();
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains("edge location must exactly match pattern"));
        }
    }
    
    /**
     * Test location validator validate location match pattern
     */
    @Test
    public void testLocationValidator2() {
        try {
            String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
            CreateMitigationRequest request = new CreateMitigationRequest();
            request.setMitigationName("BLACKWATCH_POP_GLOBAL_G-IAD55");
            request.setMitigationTemplate(mitigationTemplate);
            request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
            request.setServiceName(ServiceName.Edge);
            request.setLocations(Arrays.asList("mrs50"));
            S3Object config = new S3Object();
            config.setBucket("s3bucket");
            config.setKey("s3key");
            config.setMd5("md5");
            BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
            constraint.setConfig(config);
            MitigationDefinition mitigationDefinition = new MitigationDefinition();
            mitigationDefinition.setConstraint(constraint);
            request.setMitigationDefinition(mitigationDefinition);
            validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
            Assert.fail();
        } catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains("edge location must exactly match pattern"));
        }
    }
 
    /**
     * Create global mitigation at gamma location
     */
    @Test
    public void testCreateMitigationSuccessGammaLocation() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_G-IAD55");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("G-IAD55"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
        verify(edgeLocationsHelper, times(1)).getAllClassicPOPs();
    }
    
    /**
     * Create global mitigation at gamma predefined location
     */
    @Test
    public void testCreateMitigationSuccessGammaPredefinedLocation() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_LOADTEST-118");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("LOADTEST-118"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
    
    /**
     * Create global mitigation at non exist gamma location
     */
    @Test
    public void testCreateMitigationFailGammaPredefinedLocation() {
    	try {
	        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
	        CreateMitigationRequest request = new CreateMitigationRequest();
	        request.setMitigationName("BLACKWATCH_POP_GLOBAL_LOADTEST-118");
	        request.setMitigationTemplate(mitigationTemplate);
	        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
	        request.setServiceName(ServiceName.Edge);
	        request.setLocations(Arrays.asList("LOADTEST-1000"));
	        S3Object config = new S3Object();
	        config.setBucket("s3bucket");
	        config.setKey("s3key");
	        config.setMd5("md5");
	        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
	        constraint.setConfig(config);
	        MitigationDefinition mitigationDefinition = new MitigationDefinition();
	        mitigationDefinition.setConstraint(constraint);
	        request.setMitigationDefinition(mitigationDefinition);
	        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
	        Assert.fail();
    	} catch (IllegalArgumentException ex) {
    		Assert.assertTrue(ex.getMessage().contains("edge location must exactly match pattern"));
    	}
    }
    
    /**
     * Create global mitigation at prod location
     */
    @Test
    public void testCreateMitigationSuccess1() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("E-AMS1"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
        verify(edgeLocationsHelper, times(1)).getAllClassicPOPs();
    }
    
    /**
     * Create override mitigation
     */
    @Test
    public void testCreateMitigationSuccess3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("E-AMS1"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
        verify(edgeLocationsHelper, times(1)).getAllClassicPOPs();
    }
    
    /**
     * Create global mitigation template name
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFail1() {
        validator.validateRequestForTemplate(null, null, tsdMetric);
    }
    
    /**
     * Create global mitigation without request
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFail2() {
        validator.validateRequestForTemplate(null, MitigationTemplate.BlackWatchPOP_EdgeCustomer, tsdMetric);
    }
     
    /**
     * create mitigation without post deployment check
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFailed5() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_E-AMS50");
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("E-AMS50"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
    
    /**
     * create mitigation without configuration
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFailed6() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("E-AMS1"));
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
    
    /**
     * Create pop override mitigation but didn't specify location
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFailed7() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
 
    /**
     * Create mitigation with location is not a valid edge location
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFailed8() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_AMZ1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("AMZ1"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
    
    /**
     * Edit global mitigation 
     */
    @Test
    public void testEditMitigationSuccess2() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_E-NRT54");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocation(Arrays.asList("E-NRT54"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
        verify(edgeLocationsHelper, times(1)).getAllClassicPOPs();
    }
    
    /**
     * Edit override mitigation
     */
    @Test
    public void testEditMitigationSuccess3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        request.setLocation(Arrays.asList("E-AMS1"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
        verify(edgeLocationsHelper, times(1)).getAllClassicPOPs();
    }
    
    /**
     * delete mitigation is supported
     */
    @Test
    public void testDeleteMitigation() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setServiceName(ServiceName.Edge);
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
}

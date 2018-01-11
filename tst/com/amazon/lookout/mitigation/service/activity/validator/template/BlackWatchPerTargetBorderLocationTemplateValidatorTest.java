package com.amazon.lookout.mitigation.service.activity.validator.template;

import static org.mockito.Mockito.doReturn;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

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
import com.amazonaws.services.s3.AmazonS3;

public class BlackWatchPerTargetBorderLocationTemplateValidatorTest {
    
    @Mock
    private MetricsFactory metricsFactory;
    
    @Mock
    protected Metrics metrics;
    
    @Mock
    protected AmazonS3 s3Client;
    
    @Mock
    protected TSDMetrics tsdMetric;
    
    @Mock
    protected BlackWatchBorderLocationValidator blackWatchBorderLocationValidator;
    
    private BlackWatchPerTargetBorderLocationTemplateValidator validator;
    
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
        validator = new BlackWatchPerTargetBorderLocationTemplateValidator(s3Client,
                blackWatchBorderLocationValidator);
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
        request.setLocation("G-IAD55");
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
     * Create global mitigation at prod location
     */
    @Test
    public void testCreateMitigationSuccess1() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setLocation("E-AMS1");
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
     * Create override mitigation
     */
    @Test
    public void testCreateMitigationSuccess3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setLocation("E-AMS1");
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
        request.setLocation("E-AMS50");
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
        request.setLocation("E-AMS1");
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
        request.setLocation("E-NRT54");
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
     * Edit override mitigation
     */
    @Test
    public void testEditMitigationSuccess3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        request.setLocation("E-AMS1");
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
     * delete mitigation is supported
     */
    @Test
    public void testDeleteMitigation() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_E-AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
        validator.validateRequestForTemplate(request, mitigationTemplate, tsdMetric);
    }
    
    /**
     * Test invalid location
     */
    @Test
    public void testInvalidLocation() {
        IllegalArgumentException expectedException = new IllegalArgumentException();
        try {
            String location = "E-AMS1";
            Mockito.doThrow(expectedException).when(blackWatchBorderLocationValidator).validateLocation(location);
            String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
            CreateMitigationRequest request = new CreateMitigationRequest();
            request.setMitigationName("BLACKWATCH_POP_GLOBAL_E-AMS1");
            request.setMitigationTemplate(mitigationTemplate);
            request.setPostDeploymentChecks(Arrays.asList(ALARM_CHECK));
            request.setLocation(location);
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
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals(expectedException, ex);
        }
    }
}

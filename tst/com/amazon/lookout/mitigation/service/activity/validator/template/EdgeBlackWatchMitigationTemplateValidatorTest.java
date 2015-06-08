package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.S3Object;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;

public class EdgeBlackWatchMitigationTemplateValidatorTest {
    
    @Mock
    private EdgeLocationsHelper edgeLocationsHelper;
    
    @Mock
    private MetricsFactory metricsFactory;
    
    @Mock
    protected Metrics metrics;
    
    private EdgeBlackWatchMitigationTemplateValidator validator;
    
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        // mock TSDMetric
        doReturn(metrics).when(metricsFactory).newMetrics();
        doReturn(metrics).when(metrics).newMetrics();
        validator = new EdgeBlackWatchMitigationTemplateValidator(edgeLocationsHelper, metricsFactory);
        
        doReturn(new HashSet<String>(Arrays.asList("AMS1", "AMS50", "NRT54"))).when(edgeLocationsHelper).getBlackwatchClassicPOPs();
    }

    /**
     * Create global mitigation without locations
     */
    @Test
    public void testCreateMitigationSuccess1() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
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
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
 
    /**
     * Create global mitigation with certain location
     */
    @Test
    public void testCreateMitigationSuccess2() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("NRT54"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
    
    /**
     * Create override mitigation
     */
    @Test
    public void testCreateMitigationSuccess3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
        request.setServiceName(ServiceName.Edge);
        request.setLocations(Arrays.asList("AMS1"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
    
    /**
     * Create global mitigation template name
     */
    @Test(expected = NullPointerException.class)
    public void testCreateMitigationFail1() {
        validator.validateRequestForTemplate(null, null);
    }
    
    /**
     * Create global mitigation without request
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFail2() {
        validator.validateRequestForTemplate(null, MitigationTemplate.BlackWatchPOP_EdgeCustomer);
    }
    
    /**
     * Create mitigation with invalid name
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFailed3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACK____WATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
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
        validator.validateRequestForTemplate(request, mitigationTemplate);
    }
    
    /**
     * create mitigation without post deployment check
     */
    @Test(expected = NullPointerException.class)
    public void testCreateMitigationFailed4() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
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
        validator.validateRequestForTemplate(request, mitigationTemplate);
    }
    
    /**
     * create mitigation without configuration
     */
    @Test(expected = NullPointerException.class)
    public void testCreateMitigationFailed5() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
        request.setServiceName(ServiceName.Edge);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate);
    }
    
    /**
     * Create pop override mitigation but didn't specify location
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCreateMitigationFailed6() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
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
        validator.validateRequestForTemplate(request, mitigationTemplate);
    }
    
    /**
     * Edit global mitigation without locations
     */
    @Test
    public void testEditMitigationSuccess1() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
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
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
 
    /**
     * Edit global mitigation with certain location
     */
    @Test
    public void testEditMitigationSuccess2() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_GLOBAL");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
        request.setServiceName(ServiceName.Edge);
        request.setLocation(Arrays.asList("NRT54"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
    
    /**
     * Edit override mitigation
     */
    @Test
    public void testEditMitigationSuccess3() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
        request.setServiceName(ServiceName.Edge);
        request.setLocation(Arrays.asList("AMS1"));
        S3Object config = new S3Object();
        config.setBucket("s3bucket");
        config.setKey("s3key");
        config.setMd5("md5");
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        constraint.setConfig(config);
        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        mitigationDefinition.setConstraint(constraint);
        request.setMitigationDefinition(mitigationDefinition);
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
    
    /**
     * delete mitigation is not supported
     */
    @Test(expected = IllegalArgumentException.class)
    public void testDeleteMitigation() {
        String mitigationTemplate = MitigationTemplate.BlackWatchPOP_EdgeCustomer;
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName("BLACKWATCH_POP_OVERRIDE_AMS1");
        request.setMitigationTemplate(mitigationTemplate);
        request.setPostDeploymentChecks(Arrays.asList(mock(AlarmCheck.class)));
        request.setServiceName(ServiceName.Edge);
        validator.validateRequestForTemplate(request, mitigationTemplate);
        verify(edgeLocationsHelper, times(1)).getBlackwatchClassicPOPs();
    }
 
}
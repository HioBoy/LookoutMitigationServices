package com.amazon.lookout.mitigation.service.activity.validator.template;

import static com.amazon.lookout.mitigation.service.workflow.SWFWorkflowStarterImpl.BLACKWATCH_WORKFLOW_COMPLETION_TIMEOUT_SECONDS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.S3Object;
import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.TrafficFilterConfiguration;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazonaws.AmazonServiceException.ErrorType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.IOUtils;

public abstract class BlackWatchMitigationTemplateValidator implements DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(BlackWatchMitigationTemplateValidator.class);
    
    protected static final int MAX_ALARM_CHECK_PERIOD_SEC = 1800;
    protected static final int MAX_ALARM_CHECK_DELAY_SEC = 1200;
    static {
        // verify alarm check time is less than the workflow timeout time, and
        // have at least 5 minutes left for other execution
        assert (MAX_ALARM_CHECK_PERIOD_SEC + MAX_ALARM_CHECK_DELAY_SEC + 300) < BLACKWATCH_WORKFLOW_COMPLETION_TIMEOUT_SECONDS;
    }
    private static final String BLACKWATCH_TRAFFIC_FILTER = "traffic_filter_config";
    protected AmazonS3 blackWatchConfigS3Client;

    public BlackWatchMitigationTemplateValidator(AmazonS3 blackWatchConfigS3Client) {
        super();
        this.blackWatchConfigS3Client = blackWatchConfigS3Client;
    }
    
    @Override
    public void validateRequestForTemplate(MitigationModificationRequest request, String mitigationTemplate, TSDMetrics tsdMetric) {
        Validate.notEmpty(mitigationTemplate, "mitigation template can not be empty");

        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper
                .getDeviceNameAndScopeForTemplate(mitigationTemplate);

        if (deviceNameAndScope == null) {
            String message = String.format(
                    "%s: No DeviceNameAndScope mapping found for template: %s. Request being validated: %s",
                    MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY, mitigationTemplate,
                    ReflectionToStringBuilder.toString(request, new RecursiveToStringStyle()));
            LOG.error(message);
            throw new InternalServerError500(message);
        }

        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceNameAndScope, tsdMetric);
    }
    
    protected void validateDeploymentChecks(MitigationModificationRequest request) {
        validateNoDeploymentChecks(request.getPreDeploymentChecks(), request.getMitigationTemplate(), DeploymentCheckType.PRE);
        List<MitigationDeploymentCheck> postDeploymentChecks = request.getPostDeploymentChecks();
        if (postDeploymentChecks == null ){
            throw new IllegalArgumentException("Missing post deployment for blackwatch mitigation deployment");
        }

        Validate.notEmpty(postDeploymentChecks, "Missing post deployment for blackwatch mitigation deployment");
        
        for (MitigationDeploymentCheck check : postDeploymentChecks) {
            Validate.isTrue(check instanceof AlarmCheck, String.format("BlackWatch mitigation post deployment check "
                    + "only supports alarm check, but found %s", check));
            AlarmCheck alarmCheck = (AlarmCheck) check;
            Validate.isTrue(alarmCheck.getCheckEveryNSec() > 0, "Alarm check interval must be positive.");
            Validate.isTrue(alarmCheck.getCheckTotalPeriodSec() > 0, "Alarm check total period time must be positive.");
            Validate.isTrue(alarmCheck.getCheckTotalPeriodSec() <= MAX_ALARM_CHECK_PERIOD_SEC,
                    String.format("Alarm check total period time must be <= %d seconds.", MAX_ALARM_CHECK_PERIOD_SEC));
            Validate.isTrue(alarmCheck.getDelaySec() >= 0, "Alarm check delay time can not be negative.");
            Validate.isTrue(alarmCheck.getDelaySec() <= MAX_ALARM_CHECK_DELAY_SEC,
                    String.format("Alarm check delay time must be <= %d seconds.", MAX_ALARM_CHECK_DELAY_SEC));
            Validate.isTrue(alarmCheck.getCheckTotalPeriodSec() > alarmCheck.getCheckEveryNSec(),
                    "Alarm check total time must be larger than alarm check interval.");

            if (alarmCheck.getAlarms() == null || alarmCheck.getAlarms().isEmpty()){
                throw new IllegalArgumentException(String.format("Found empty map of alarms %s",
                        ReflectionToStringBuilder.toString(alarmCheck, new RecursiveToStringStyle())));
            }

            for (String alarmType : alarmCheck.getAlarms().keySet()) {
                Validate.notEmpty(alarmCheck.getAlarms().get(alarmType),
                        String.format("Found empty alarm list for alarm type %s.", alarmType));
            }
        }
    }
    
    protected void validateBlackWatchConfigBasedConstraint(Constraint constraint) {
        Validate.isTrue(constraint instanceof BlackWatchConfigBasedConstraint,
                "BlackWatch mitigationDefinition must contain single constraint of type BlackWatchConfigBasedConstraint.");
        
        BlackWatchConfigBasedConstraint blackWatchConfig = (BlackWatchConfigBasedConstraint)constraint;
        
        S3Object config = blackWatchConfig.getConfig();
        Validate.notNull(config);
        Validate.notEmpty(config.getBucket(), "BlackWatch Config S3 object missing s3 bucket");
        Validate.notEmpty(config.getKey(), "BlackWatch Config S3 object missing s3 object key");
        Validate.notEmpty(config.getMd5(), "BlackWatch Config S3 object missing s3 object md5 checksum");
        
        if (blackWatchConfig.getConfigData() != null) {
            for (S3Object configData : blackWatchConfig.getConfigData()) {
                Validate.notEmpty(configData.getBucket(), 
                        String.format("BlackWatch Config Data S3 object [%s] missing s3 bucket",
                                ReflectionToStringBuilder.toString(configData)));
                Validate.notEmpty(configData.getKey(), 
                        String.format("BlackWatch Config Data S3 object [%s] missing s3 object key",
                                ReflectionToStringBuilder.toString(configData)));
                Validate.notEmpty(configData.getMd5(), 
                        String.format("BlackWatch Config Data S3 object [%s] missing s3 object md5 checksum",
                                ReflectionToStringBuilder.toString(configData)));
                
                if (configData.getKey().contains(BLACKWATCH_TRAFFIC_FILTER)) {
                    validateBlackWatchTrafficFilterConfig(configData);
                }
            }
        }
    }
    
    private void validateBlackWatchTrafficFilterConfig(S3Object config) {
        try {
            // download configuration from s3
            com.amazonaws.services.s3.model.S3Object s3Object = 
                    blackWatchConfigS3Client.getObject(new GetObjectRequest(config.getBucket(), config.getKey()));
            InputStream objectData = s3Object.getObjectContent();
            String originConfig = IOUtils.toString(objectData);
            objectData.close();
            
            // process configuration
            String processedConfig = TrafficFilterConfiguration.processConfiguration(originConfig);
            
            // if configuration is different, update it with new value
            if (!TrafficFilterConfiguration.isSameConfig(originConfig, processedConfig)) {
                // upload the processed configuration content to s3 as a new object
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(processedConfig.length());
                InputStream objectStream = new ByteArrayInputStream(processedConfig.getBytes(StandardCharsets.UTF_8));
                // the config object will be modified to hold the processed configuration s3 object
                PutObjectRequest request = new PutObjectRequest(config.getBucket(), config.getKey(), objectStream, metadata);
                PutObjectResult response = blackWatchConfigS3Client.putObject(request);
                objectStream.close();
                // md5 checksum
                config.setMd5(response.getETag());
            }
        } catch (IOException io) {
            throw new InternalServerError500(String.format(
                    "Failed to fetch traffic filter content from s3. s3 object %s",
                    ReflectionToStringBuilder.toString(config)), io);
        } catch (AmazonS3Exception ex) {
            if (ex.getErrorType().equals(ErrorType.Client)) {
                throw new IllegalArgumentException(String.format(
                        "Failed to fetch traffic filter content from s3. s3 object %s",
                        ReflectionToStringBuilder.toString(config)), ex);
            } else {
                throw new InternalServerError500(String.format(
                        "Failed to fetch traffic filter content from s3. s3 object %s",
                        ReflectionToStringBuilder.toString(config)), ex);
            }
        }
    }

    @Override
    public void validateCoexistenceForTemplateAndDevice(String templateForNewDefinition,
            String mitigationNameForNewDefinition, MitigationDefinition newDefinition,
            String templateForExistingDefinition, String mitigationNameForExistingDefinition,
            MitigationDefinition existingDefinition, TSDMetrics metrics) {
        // blackwatch allow multiple mitigations, so ignore this check.
        // it also allow same mitigation definition but different mitigation name.
        // so leave this empty.
    } 
}

package com.amazon.lookout.mitigation.service.activity.validator.template;

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
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
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

    private static final String BLACKWATCH_TRAFFIC_FILTER = "traffic_filter_config";

    @Override
    public void validateRequestForTemplate(MitigationModificationRequest request, String mitigationTemplate, TSDMetrics tsdMetric) {
        Validate.notEmpty(mitigationTemplate, "mitigation template can not be empty");

        DeviceName deviceName = MitigationTemplateToDeviceMapper
                .getDeviceNameForTemplate(mitigationTemplate);

        if (deviceName == null) {
            String message = String.format(
                    "%s: No DeviceName mapping found for template: %s. Request being validated: %s",
                    MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY, mitigationTemplate,
                    ReflectionToStringBuilder.toString(request, new RecursiveToStringStyle()));
            LOG.error(message);
            throw new InternalServerError500(message);
        }

        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceName, tsdMetric);
    }

    protected void validateDeploymentChecks(MitigationModificationRequest request) {
        if (!(request.getPreDeploymentChecks() == null || request.getPreDeploymentChecks().isEmpty())) {
            throw new IllegalArgumentException("Pre-deployment checks are not supported");
        }

        List<MitigationDeploymentCheck> postDeploymentChecks = request.getPostDeploymentChecks();
        if (postDeploymentChecks == null || postDeploymentChecks.isEmpty()) {
            //we allow post deployment check to be empty for the blackwatch border template
            if (request.getMitigationTemplate().equals(MitigationTemplate.BlackWatchBorder_PerTarget_AWSCustomer) ||
                    request.getMitigationTemplate().equals(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer) ||
                    request.getMitigationTemplate().equals(MitigationTemplate.Vanta)) {
                return;
            }
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

            if (alarmCheck.getAlarms() == null || alarmCheck.getAlarms().isEmpty()) {
                throw new IllegalArgumentException(String.format("Found empty map of alarms %s",
                        ReflectionToStringBuilder.toString(alarmCheck, new RecursiveToStringStyle())));
            }

            for (String alarmType : alarmCheck.getAlarms().keySet()) {
                Validate.notEmpty(alarmCheck.getAlarms().get(alarmType),
                        String.format("Found empty alarm list for alarm type %s.", alarmType));
            }
        }
    }

    static void validateS3Object(S3Object s3Object, String usageDescription) {
        Validate.notNull(s3Object);

        if (s3Object.getBucket() != null) {
            // BlackWatchAgentPython allows an s3 object's bucket to be missing,
            // in which case it downloads the object from the same S3 bucket it
            // downloads mitigation definitions from, but it cannot be set to an
            // empty value.
            Validate.notEmpty(s3Object.getBucket(),
                    String.format("%s S3 object [%s] empty value for s3 bucket",
                            usageDescription, ReflectionToStringBuilder.toString(s3Object)));
        }

        Validate.notEmpty(s3Object.getKey(),
                String.format("%s S3 object [%s] missing s3 key",
                        usageDescription, ReflectionToStringBuilder.toString(s3Object)));

        if (s3Object.isEnableRefresh()) {
            Validate.isTrue(s3Object.getMd5() == null,
                    String.format("%s S3 object [%s] with refresh enabled has object md5 checksum set",
                            usageDescription, ReflectionToStringBuilder.toString(s3Object)));
            Validate.isTrue(s3Object.getSha256() == null,
                    String.format("%s S3 object [%s] with refresh enabled has object sha256 checksum set",
                            usageDescription, ReflectionToStringBuilder.toString(s3Object)));

            if (s3Object.getRefreshInterval() != null) {
                Validate.isTrue(s3Object.getRefreshInterval() > 0,
                        String.format("%s S3 object [%s] refresh interval [%s] is not greater than 0",
                                usageDescription, ReflectionToStringBuilder.toString(s3Object),
                                s3Object.getRefreshInterval()));
            }
        } else {
            //must have either md5 or sha256 set.
            Validate.isTrue((s3Object.getMd5() != null || s3Object.getSha256() != null),
                    String.format("%s S3 object [%s] missing both md5 and sha256 checksum",
                            usageDescription, ReflectionToStringBuilder.toString(s3Object)));

            Validate.isTrue(s3Object.getRefreshInterval() == null,
                    String.format("%s S3 object [%s] with refresh disabled has refresh interval set",
                            usageDescription, ReflectionToStringBuilder.toString(s3Object)));
        }
    }

    protected void validateBlackWatchConfigBasedConstraint(Constraint constraint) {
        Validate.isTrue(constraint instanceof BlackWatchConfigBasedConstraint,
                "BlackWatch mitigationDefinition must contain single constraint of type BlackWatchConfigBasedConstraint.");

        BlackWatchConfigBasedConstraint blackWatchConfig = (BlackWatchConfigBasedConstraint) constraint;

        S3Object config = blackWatchConfig.getConfig();
        validateS3Object(config, "BlackWatch Config");

        if (blackWatchConfig.getConfigData() != null) {
            for (S3Object configData : blackWatchConfig.getConfigData()) {
                validateS3Object(configData, "BlackWatch Config Data");
            }
        }
    }
}



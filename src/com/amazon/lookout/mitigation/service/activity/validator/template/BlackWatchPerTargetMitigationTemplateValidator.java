package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.lang.Validate;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.RollbackMitigationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazonaws.services.s3.AmazonS3;

/**
 * This template is for BlackWatch per target mitigation.
 * @author xingbow
 *
 */
public abstract class BlackWatchPerTargetMitigationTemplateValidator extends BlackWatchMitigationTemplateValidator {
    private static final Pattern MITIGATION_NAME_VALIDATION_REGEX = Pattern.compile("[\\w-_=\\.]{1,255}");
    
    public BlackWatchPerTargetMitigationTemplateValidator(AmazonS3 blackWatchConfigS3Client) {
        super(blackWatchConfigS3Client);
    }

    @Override
    public void validateRequestForTemplateAndDevice(
            MitigationModificationRequest request, String mitigationTemplate,
            DeviceName deviceName, TSDMetrics tsdMetric) {
        Validate.notEmpty(mitigationTemplate, "mitigation template can not be empty");
        Validate.notNull(request, "missing request definition");
        Validate.notEmpty(request.getMitigationName(), "missing mitigation name");
        Validate.isTrue(MITIGATION_NAME_VALIDATION_REGEX.matcher(request.getMitigationName()).matches(),
                "mitigation name does not match pattern " + MITIGATION_NAME_VALIDATION_REGEX.pattern());

        if (request instanceof CreateMitigationRequest) {
            validateCreateRequest((CreateMitigationRequest) request);
        } else if (request instanceof EditMitigationRequest) {
            validateEditRequest((EditMitigationRequest) request);
        } else if (request instanceof DeleteMitigationFromAllLocationsRequest) {
            validateDeleteRequest((DeleteMitigationFromAllLocationsRequest) request);
        } else if (request instanceof RollbackMitigationRequest) {
            validateRollbackRequest((RollbackMitigationRequest) request);
        } else {
            throw new IllegalArgumentException(
                    String.format("Not supported Request type for mitigation template %s", mitigationTemplate));
        }
    }
    
    private void validateRollbackRequest(RollbackMitigationRequest request) {
        validateDeploymentChecks(request);
    }
    
    private void validateDeleteRequest(
            DeleteMitigationFromAllLocationsRequest request) {
        validateDeploymentChecks(request);
    }

    private void validateEditRequest(EditMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");
        validateLocation(request.getLocation());
        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        validateDeploymentChecks(request);
    }

    private void validateCreateRequest(CreateMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");
        validateLocation(request.getLocation());
        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        validateDeploymentChecks(request);
    }

    protected abstract void validateLocation(final String location);
}


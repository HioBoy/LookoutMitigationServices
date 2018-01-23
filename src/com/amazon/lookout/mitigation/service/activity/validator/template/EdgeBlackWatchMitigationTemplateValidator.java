package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazonaws.services.s3.AmazonS3;
/**
 * Validate Edge BlackWatch mitigation request.
 * 
 * Each pop will have 1 mandatory "BLACKWATCH_POP_GLOBAL_<POP>" mitigation for global configuration, 
 * and 1 optional "BLACKWATCH_POP_OVERRIDE_<POP>" mitigation for per pop override configuration. 
 * The global configuration and per pop override configuration are exactly same as the current one in LookoutBlackWatchConfig package.
 * The the <POP> will be checked against the real edge pop name, and it has to match the locations in the request passed in.
 * 
 * We only support 2 kinds of operations. Delete mitigation is not supported
 * 1. Create mitigation
 * 2. Edit mitigation
 * 
 * @author xingbow
 *
 */
public class EdgeBlackWatchMitigationTemplateValidator extends BlackWatchMitigationTemplateValidator {
    private static final Log LOG = LogFactory.getLog(EdgeBlackWatchMitigationTemplateValidator.class);

    private static final String LOCATION_PATTERN = "[-_A-Za-z0-9]+";
    private static final Pattern VALID_GLOBAL_MITIGATION_NAME_PATTERN = Pattern.compile(String.format("BLACKWATCH_POP_GLOBAL_(%s)", LOCATION_PATTERN));
    private static final Pattern VALID_POP_OVERRIDE_MITIGATION_NAME_PATTERN = Pattern.compile(String.format("BLACKWATCH_POP_OVERRIDE_(%s)", LOCATION_PATTERN));
    
    public EdgeBlackWatchMitigationTemplateValidator(AmazonS3 blackWatchConfigS3Client) {
        super(blackWatchConfigS3Client);
    }
    
    @Override
    public void validateRequestForTemplateAndDevice(
            MitigationModificationRequest request, String mitigationTemplate,
            DeviceName deviceName,
            TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate, "mitigation template can not be empty");

        if (request instanceof CreateMitigationRequest) {
            validateCreateRequest((CreateMitigationRequest) request);
        } else if (request instanceof EditMitigationRequest) {
            validateEditRequest((EditMitigationRequest) request);
        } else {
            throw new IllegalArgumentException(
                    String.format("request %s is not supported for mitigation template %s", request, mitigationTemplate));
        }
    }

    private void validateCreateRequest(CreateMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        validateDeploymentChecks(request);
    }
   
    private void validateEditRequest(EditMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        validateDeploymentChecks(request);
    }

    private String findLocationFromMitigationName(String mitigationName) {
        Validate.notEmpty(mitigationName, "mitigationName cannot be null or empty.");

        Matcher globalMitigationMatcher = VALID_GLOBAL_MITIGATION_NAME_PATTERN.matcher(mitigationName);
        Matcher popOverrideMitigationMatcher = VALID_POP_OVERRIDE_MITIGATION_NAME_PATTERN.matcher(mitigationName);
        if (globalMitigationMatcher.find()) {
            return globalMitigationMatcher.group(1);
        }
        if (popOverrideMitigationMatcher.find()) {
            return popOverrideMitigationMatcher.group(1);
        }
        String message = String.format("Invalid mitigationName %s. Name doesn't match the any mitigation name pattern.", mitigationName);
        LOG.info(message);
        throw new IllegalArgumentException(message);
    }
}


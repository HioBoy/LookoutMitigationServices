package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AllArgsConstructor;

import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.S3Object;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
@AllArgsConstructor
public class EdgeBlackWatchMitigationTemplateValidator implements DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(EdgeBlackWatchMitigationTemplateValidator.class);

    private static final String LOCATION_PATTERN = "[-_A-Za-z0-9]+";
    private static final Pattern PROD_LOCATION_PATTERN = Pattern.compile(String.format("E-([A-Z0-9]+)"));
    private static final Pattern VALID_GLOBAL_MITIGATION_NAME_PATTERN = Pattern.compile(String.format("BLACKWATCH_POP_GLOBAL_(%s)", LOCATION_PATTERN));
    private static final Pattern VALID_POP_OVERRIDE_MITIGATION_NAME_PATTERN = Pattern.compile(String.format("BLACKWATCH_POP_OVERRIDE_(%s)", LOCATION_PATTERN));
    
    private final EdgeLocationsHelper edgeLocationsHelper;
    
    @Override
    public void validateRequestForTemplate(
            MitigationModificationRequest request, String mitigationTemplate) {
        Validate.notEmpty(mitigationTemplate, "mitigation template can not be empty");
        
        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(mitigationTemplate);
        
        if (deviceNameAndScope == null) {
            String message = String.format(
                    "%s: No DeviceNameAndScope mapping found for template: %s. Request being validated: %s",
                    MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY, mitigationTemplate,
                    ReflectionToStringBuilder.toString(request));
            LOG.error(message);
            throw new InternalServerError500(message);
        }

        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceNameAndScope);
    }

    @Override
    public void validateRequestForTemplateAndDevice(
            MitigationModificationRequest request, String mitigationTemplate,
            DeviceNameAndScope deviceNameAndScope) {
        Validate.notEmpty(mitigationTemplate, "mitigation template can not be empty");

        if (request instanceof CreateMitigationRequest) {
            validateCreateRequest((CreateMitigationRequest) request);
        } else if (request instanceof EditMitigationRequest) {
            validateEditRequest((EditMitigationRequest) request);
        } else if (request instanceof DeleteMitigationFromAllLocationsRequest) {
            throw new IllegalArgumentException(
                    String.format("Delete not supported for mitigation template %s", mitigationTemplate));
        } else {
            throw new IllegalArgumentException(
                    String.format("request %s is Not supported for mitigation template %s", request, mitigationTemplate));
        }
    }

    private void validateCreateRequest(CreateMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        String location = findLocationFromMitigationName(request.getMitigationName());
        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        validateLocations(location, request.getMitigationName(), request.getLocations());
        validatePostDeploymentChecks(request.getPostDeploymentChecks());
    }   

    private void validateEditRequest(EditMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        String location = findLocationFromMitigationName(request.getMitigationName());
        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        // TODO fix location to be locations in model in a separate commit
        validateLocations(location, request.getMitigationName(), request.getLocation());
        validatePostDeploymentChecks(request.getPostDeploymentChecks());
    }
        
    private void validateLocations(String location, String mitigationName, List<String> locations) {
        Validate.notEmpty(locations, String.format("locations %s should not be empty.", locations));
        Validate.isTrue(locations.size() == 1, String.format("locations %s should only contains 1 location.", locations));
        Validate.isTrue(location.equals(locations.get(0)), String.format("locations %s should match the location %s found in mitigation name.", locations, location));

        // translate prod location style from E-MRS50 to MRS50, so it can be same style as the locations fetched from edge location helper
        Matcher prodLocationMatcher = PROD_LOCATION_PATTERN.matcher(location);
        if (prodLocationMatcher.find()) {
            location = prodLocationMatcher.group(1);
        }

        Validate.isTrue(edgeLocationsHelper.getAllClassicPOPs().contains(location), String.format("location %s is not a valid edge location.", location));
    }
    
    private void validatePostDeploymentChecks(List<MitigationDeploymentCheck> checks) {
        Validate.notEmpty(checks, "Missing post deployment for blackwatch mitigation deployment");
        
        for (MitigationDeploymentCheck check : checks) {
            Validate.isInstanceOf(AlarmCheck.class, check, String.format("BlackWatch mitigation post deployment check "
                    + "only support alarm check, but found %s", check));
            AlarmCheck alarmCheck = (AlarmCheck) check;
            Validate.isTrue(alarmCheck.getCheckEveryNSec() > 0, "Alarm check interval must be positive.");
            Validate.isTrue(alarmCheck.getCheckTotalPeriodSec() > 0, "Alarm check total period time must be positive.");
            Validate.isTrue(alarmCheck.getDelaySec() >= 0, "Alarm check delay time can not be negative.");
            Validate.isTrue(alarmCheck.getCheckTotalPeriodSec() > alarmCheck.getCheckEveryNSec(), "Alarm check total time must be larger than alarm check interval.");
            for (String alarmType : alarmCheck.getAlarms().keySet()) {
                Validate.notEmpty(alarmCheck.getAlarms().get(alarmType), String.format("Found empty alarm list for alamr type %s.", alarmType));
            }
        }
    }
 
    private void validateBlackWatchConfigBasedConstraint(Constraint constraint) {
        Validate.isInstanceOf(BlackWatchConfigBasedConstraint.class, constraint,
                "BlackWatch mitigationDefinition must contain single constraint of type BlackWatchConfigBasedConstraint.");
        
        S3Object config = ((BlackWatchConfigBasedConstraint)constraint).getConfig();
        Validate.notNull(config);
        Validate.notEmpty(config.getBucket(), "BlackWatch Config S3 object missing s3 bucket");
        Validate.notEmpty(config.getKey(), "BlackWatch Config S3 object missing s3 object key");
        Validate.notEmpty(config.getMd5(), "BlackWatch Config S3 object missing s3 object md5 checksum");
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

    @Override
    public void validateCoexistenceForTemplateAndDevice(
            String templateForNewDefinition,
            String mitigationNameForNewDefinition,
            MitigationDefinition newDefinition,
            String templateForExistingDefinition,
            String mitigationNameForExistingDefinition,
            MitigationDefinition existingDefinition) {
        // blackwatch allow multiple mitigations, so ignore this check.
    }

    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Edge;
    }
}

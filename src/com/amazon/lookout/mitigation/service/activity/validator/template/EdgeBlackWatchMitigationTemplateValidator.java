package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.AllArgsConstructor;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.metrics.MetricsFactory;
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
 * For pop, we currently only allow 2 kinds of mitigations.
 * 1. global mitigation, which has name as "BLACKWATCH_POP_GLOBAL", which is deployed at all pop locations.
 * 2. pop override mitigation, which has name as "BLACKWATCH_POP_OVERRIDE_<POP>", which only deployed to corresponding pop location
 * 
 * We only support 2 kinds of operations. Delete mitigation is not supported
 * 1. Create mitigation
 * 2. Edit mitigation
 * 
 * For global mitigaiton, if the request does not contains location, 
 * it will be deployed to all the pop locations that has blackwatch up and running.
 * If a location is given, then it will be deployed to a certain set of locations.
 * Each location will be checked against edge pop locations.
 * 
 * @author xingbow
 *
 */
@AllArgsConstructor
public class EdgeBlackWatchMitigationTemplateValidator implements
        DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(EdgeBlackWatchMitigationTemplateValidator.class);

    private static final Pattern VALID_GLOBAL_MITIGATION_NAME_PATTERN = Pattern.compile("BLACKWATCH_POP_GLOBAL");
    private static final Pattern VALID_POP_OVERRIDE_MITIGATION_NAME_PATTERN = Pattern.compile("BLACKWATCH_POP_OVERRIDE_([A-Z0-9]+)");
    
    private final EdgeLocationsHelper edgeLocationsHelper;
    private final MetricsFactory metricsFactory;
    
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

        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        validateLocations(request.getMitigationName(), request.getLocations());
        validatePostDeploymentChecks(request.getPostDeploymentChecks());
    }   

    private void validateEditRequest(EditMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateBlackWatchConfigBasedConstraint(request.getMitigationDefinition().getConstraint());
        // TODO fix location to be locations in model in a separate commit
        validateLocations(request.getMitigationName(), request.getLocation());
        validatePostDeploymentChecks(request.getPostDeploymentChecks());
    }
        
    private void validateLocations(String mitigationName, List<String> locations) {
        boolean isGlobalMitigation = isGlobalMitigation(mitigationName);
        try (TSDMetrics tsdMetrics = new TSDMetrics(metricsFactory, "EdgeBlackWatchMitigationTemplateValidator.validateLocations")) {
            // if global mitigation
            if (isGlobalMitigation) {
                if (locations == null || locations.isEmpty()) {
                    locations = new ArrayList<String>();
                    for (String location : edgeLocationsHelper.getBlackwatchClassicPOPs()) {
                        locations.add(location);
                    }
                    LOG.info(String.format("Found Active BlackWatch in locations %s, deploy the global mitigations to these locations", locations));
                } else {
                    Set<String> allEdgeLocations = edgeLocationsHelper.getBlackwatchClassicPOPs();
                    for (String location : locations) {
                        Validate.isTrue(allEdgeLocations.contains(location), String.format("location %s is not a valid edge location", location));
                    }
                }
            } else {
                if (locations == null || locations.size() != 1) {
                    String msg = String.format("Invalid locations %s for non-global blackwatch mitigation deployment, mitigation name %s.",
                            locations, mitigationName);
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                } else {
                    // validate mitigation name and location is consistent
                    Matcher matcher = VALID_POP_OVERRIDE_MITIGATION_NAME_PATTERN.matcher(mitigationName);
                    matcher.find();
                    String location = matcher.group(1);
                    Validate.isTrue(location.equals(locations.get(0)), 
                            String.format("location %s does not match mitigation name %s.", locations.get(0), location));
                    Validate.isTrue(edgeLocationsHelper.getBlackwatchClassicPOPs().contains(location),
                            String.format("location %s is not a blackwatch pop.", location));
                }
            }
        }
    }
    
    private void validatePostDeploymentChecks(List<MitigationDeploymentCheck> checks) {
        Validate.notEmpty(checks, "Missing post deployment for blackwatch mitigation deployment");
        
        for (MitigationDeploymentCheck check : checks) {
            Validate.isInstanceOf(AlarmCheck.class, check, String.format("BlackWatch mitigation post deployment check only support alarm check, but found %s", check));
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

    private boolean isGlobalMitigation(String mitigationName) {
        Validate.notEmpty(mitigationName, "mitigationName cannot be null or empty.");

        Matcher globalMitigationMatcher = VALID_GLOBAL_MITIGATION_NAME_PATTERN.matcher(mitigationName);
        Matcher popOverrideMitigationMatcher = VALID_POP_OVERRIDE_MITIGATION_NAME_PATTERN.matcher(mitigationName);
        boolean isGlobalMitigation = globalMitigationMatcher.find();
        if (!isGlobalMitigation && !popOverrideMitigationMatcher.find()) {
            String message = String.format("Invalid mitigationName %s. Name doesn't match the any mitigation name pattern.", mitigationName);
            LOG.info(message);
            throw new IllegalArgumentException(message);
        }
        return isGlobalMitigation;
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

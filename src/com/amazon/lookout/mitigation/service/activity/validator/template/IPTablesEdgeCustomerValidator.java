package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.NonNull;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.IPTablesJsonValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.StandardLocations;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.amazonaws.util.CollectionUtils;

public class IPTablesEdgeCustomerValidator implements DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(IPTablesEdgeCustomerValidator.class);

    private static final Pattern INVALID_MITIGATION_NAME_PATTERN = Pattern.compile("\n|\r|\u0085|\u2028|\u2029");

    private final IPTablesJsonValidator ipTablesJsonValidator;
    private final DataConverter jsonDataConverter = new JsonDataConverter();

    public IPTablesEdgeCustomerValidator(IPTablesJsonValidator ipTablesJsonValidator) {
        this.ipTablesJsonValidator = ipTablesJsonValidator;
    }

    @Override
    public void validateRequestForTemplateAndDevice(
            @NonNull MitigationModificationRequest request,
            @NonNull String mitigationTemplate,
            @NonNull DeviceName deviceName,
            @NonNull TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate);

        validateMitigationName(request.getMitigationName());
        validateNoDeploymentChecks(request.getPreDeploymentChecks(), request.getMitigationTemplate(), DeploymentCheckType.PRE);
        validateNoDeploymentChecks(request.getPostDeploymentChecks(), request.getMitigationTemplate(), DeploymentCheckType.POST);

        if (request instanceof CreateMitigationRequest) {
            final CreateMitigationRequest r = (CreateMitigationRequest) request;
            validateLocation(r.getLocation());
            validateCreateRequest(r);
        } else if (request instanceof EditMitigationRequest) {
            final EditMitigationRequest r = (EditMitigationRequest) request;
            final String location = r.getLocation();
            validateLocation(location);
            validateEditRequest(r);
        } else {
            throw new IllegalArgumentException(
                    String.format("request %s is not supported for mitigation template %s", request, mitigationTemplate));
        }
    }

    @Override
    public void validateRequestForTemplate(
            @NonNull MitigationModificationRequest request,
            @NonNull String mitigationTemplate,
            @NonNull TSDMetrics metrics) {
        Validate.notEmpty(mitigationTemplate);

        DeviceName deviceName = MitigationTemplateToDeviceMapper.getDeviceNameForTemplate(
                mitigationTemplate);
        if (deviceName == null) {
            String message = String.format(
                    "%s: No DeviceName mapping found for template: %s. Request being validated: %s",
                    MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY,
                    mitigationTemplate,
                    ReflectionToStringBuilder.toString(request));
            LOG.error(message);
            throw new InternalServerError500(message);
        }

        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceName, metrics);
    }

    /**
     * We currently check if 2 definitions are exactly identical. There could be cases where 2 definitions are equivalent, but not identical (eg:
     * when they have the same constraints, but the constraints are in different order) - in those cases we don't treat them as identical for now.
     * We could do so, by enforcing a particular ordering to the mitigation definitions when we persist the definition - however it might get tricky
     * to do so for different use-cases, eg: for IPTables maybe the user crafted rules such that they are in a certain order for a specific reason.
     * We could have a deeper-check based on the template - which checks if 2 mitigations are equivalent, but we don't have a strong use-case for such as of now, 
     * hence keeping the comparison simple for now.
     * 
     * We also first check if the hashcode for definitions matches - which acts as a shortcut to avoid deep inspection of the definitions.
     */
    @Override
    public void validateCoexistenceForTemplateAndDevice(
            @NonNull String templateForNewDefinition,
            @NonNull String mitigationNameForNewDefinition,
            @NonNull MitigationDefinition newDefinition,
            @NonNull String templateForExistingDefinition,
            @NonNull String mitigationNameForExistingDefinition,
            @NonNull MitigationDefinition existingDefinition,
            @NonNull TSDMetrics metrics) {
        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(mitigationNameForNewDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(mitigationNameForExistingDefinition);
        
        if ((existingDefinition.hashCode()== newDefinition.hashCode()) && newDefinition.equals(existingDefinition)) {
            String msg = "Found identical mitigation definition: " + mitigationNameForExistingDefinition + " for existingTemplate: " + templateForExistingDefinition +
                         " with definition: " + jsonDataConverter.toData(existingDefinition) + " for request with MitigationName: " + mitigationNameForNewDefinition + 
                         " and MitigationTemplate: " + templateForNewDefinition + " with definition: " + jsonDataConverter.toData(newDefinition);
            LOG.info(msg);
            throw new DuplicateDefinitionException400(msg);
        }

        if (templateForNewDefinition.equals(MitigationTemplate.IPTables_Mitigation_EdgeCustomer)) {
            checkForDuplicateDefinition(
                    templateForNewDefinition,
                    templateForExistingDefinition,
                    mitigationNameForExistingDefinition
            );
        }
    }
    
    @Override
    public boolean requiresCheckForDuplicateAndConflictingRequests() {
        return true;
    }

    private void validateMitigationName(String mitigationName) {
        if (mitigationName == null) {
            throw new IllegalArgumentException("mitigationName cannot be null or empty.");
        }
        Validate.notEmpty(mitigationName, "mitigationName cannot be null or empty.");

        Matcher matcher = INVALID_MITIGATION_NAME_PATTERN.matcher(mitigationName);
        if (matcher.find()) {
            String message = String.format(
                    "Invalid mitigationName %s. Name cannot contain any control characters.",
                    mitigationName);
            LOG.info(message);
            throw new IllegalArgumentException(message);
        }
    }

    private void validateLocation(String locationToDeploy) {
        Validate.notEmpty(locationToDeploy);

        if (locationToDeploy.equals(StandardLocations.EDGE_WORLD_WIDE)) {
            return;
        }

        throw new IllegalArgumentException(String.format(
            "Mitigation locations for IPTables mitigation must be null or empty or " +
            "contain a single %s location.",
            StandardLocations.EDGE_WORLD_WIDE));
    }

    private void validateCreateRequest(CreateMitigationRequest request) {
        if (request.getMitigationDefinition() == null) {
            throw new IllegalArgumentException("mitigationDefinition cannot be null.");
        }

        validateIPTablesConstraint(request.getMitigationDefinition().getConstraint());
    }

    private void validateEditRequest(EditMitigationRequest request) {
        if (request.getMitigationDefinition() == null) {
            throw new IllegalArgumentException("mitigationDefinition cannot be null.");
        }

        validateIPTablesConstraint(request.getMitigationDefinition().getConstraint());
    }

    private void validateIPTablesConstraint(Constraint constraint) {
        Validate.isInstanceOf(
                SimpleConstraint.class,
                constraint,
                "IPTables mitigationDefinition must contain single constraint of type SimpleConstraint.");

        validateIPTablesConstraintAttributes((SimpleConstraint) constraint);
    }

    private void validateIPTablesConstraintAttributes(SimpleConstraint ipTablesConstraint) {
        if (ipTablesConstraint.getAttributeValues() == null) {
            throw new IllegalArgumentException(
                    "IPTables constraint must contain single attribute value with IPTables JSON.");
        }
        Validate.notEmpty(ipTablesConstraint.getAttributeValues(),
                "IPTables constraint must contain single attribute value with IPTables JSON.");

        String ipTablesJson = ipTablesConstraint.getAttributeValues().get(0);
        ipTablesJsonValidator.validateIPTablesJson(ipTablesJson);
    }

    private void checkForDuplicateDefinition(
            String templateForNewDefinition,
            String templateForExistingDefinition,
            String mitigationNameForExistingDefinition) {
        if (templateForNewDefinition.equals(templateForExistingDefinition)) {
            String message = String.format(
                    "For MitigationTemplate: %s we can have at most 1 mitigation active at a time. " +
                            "Currently mitigation: %s already exists for this template",
                    templateForNewDefinition,
                    mitigationNameForExistingDefinition);
            LOG.info(message);
            throw new DuplicateDefinitionException400(message);
        }
    }
}

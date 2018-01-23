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

        if (!(request.getPreDeploymentChecks() == null || request.getPreDeploymentChecks().isEmpty())) {
            throw new IllegalArgumentException("Don't support pre-deployment checks");
        }

        if (!(request.getPostDeploymentChecks() == null || request.getPostDeploymentChecks().isEmpty())) {
            throw new IllegalArgumentException("Don't support post-deployment checks");
        }

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
}


package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.DuplicateDefinitionException400;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.validator.template.iptables.edgecustomer.IPTablesJsonValidator;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.constants.MitigationTemplateToDeviceMapper;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import lombok.NonNull;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPTablesEdgeCustomerValidator implements DeviceBasedServiceTemplateValidator {
    private static final Log LOG = LogFactory.getLog(IPTablesEdgeCustomerValidator.class);

    private static final Pattern INVALID_MITIGATION_NAME_PATTERN = Pattern.compile("\n|\r|\u0085|\u2028|\u2029");

    private final IPTablesJsonValidator ipTablesJsonValidator;

    public IPTablesEdgeCustomerValidator(IPTablesJsonValidator ipTablesJsonValidator) {
        this.ipTablesJsonValidator = ipTablesJsonValidator;
    }

    @Override
    public void validateRequestForTemplateAndDevice(
            @NonNull MitigationModificationRequest request,
            @NonNull String mitigationTemplate,
            @NonNull DeviceNameAndScope deviceNameAndScope) {
        Validate.notEmpty(mitigationTemplate);

        validateMitigationName(request.getMitigationName());

        if (request instanceof CreateMitigationRequest) {
            validateCreateRequest((CreateMitigationRequest) request);
        } else if (request instanceof EditMitigationRequest) {
            validateEditRequest((EditMitigationRequest) request);
        } else if (request instanceof DeleteMitigationFromAllLocationsRequest) {
            throw new IllegalArgumentException(
                    String.format("Delete not supported for mitigation template %s", mitigationTemplate));
        }
    }

    @Override
    public void validateRequestForTemplate(
            @NonNull MitigationModificationRequest request,
            @NonNull String mitigationTemplate) {
        Validate.notEmpty(mitigationTemplate);

        DeviceNameAndScope deviceNameAndScope = MitigationTemplateToDeviceMapper.getDeviceNameAndScopeForTemplate(
                mitigationTemplate);
        if (deviceNameAndScope == null) {
            String message = String.format(
                    "%s: No DeviceNameAndScope mapping found for template: %s. Request being validated: %s",
                    MitigationTemplateToDeviceMapper.MISSING_DEVICE_MAPPING_KEY,
                    mitigationTemplate,
                    ReflectionToStringBuilder.toString(request));
            LOG.error(message);
            throw new InternalServerError500(message);
        }

        validateRequestForTemplateAndDevice(request, mitigationTemplate, deviceNameAndScope);
    }

    @Override
    public void validateCoexistenceForTemplateAndDevice(
            @NonNull String templateForNewDefinition,
            @NonNull String mitigationNameForNewDefinition,
            @NonNull MitigationDefinition newDefinition,
            @NonNull String templateForExistingDefinition,
            @NonNull String mitigationNameForExistingDefinition,
            @NonNull MitigationDefinition existingDefinition) {
        Validate.notEmpty(templateForNewDefinition);
        Validate.notEmpty(mitigationNameForNewDefinition);
        Validate.notEmpty(templateForExistingDefinition);
        Validate.notEmpty(mitigationNameForExistingDefinition);

        if (templateForNewDefinition.equals(MitigationTemplate.IPTables_Mitigation_EdgeCustomer)) {
            checkForDuplicateDefinition(
                    templateForNewDefinition,
                    templateForExistingDefinition,
                    mitigationNameForExistingDefinition
            );
        }
    }

    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Edge;
    }

    private void validateMitigationName(String mitigationName) {
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

    private void validateCreateRequest(CreateMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

        validateIPTablesConstraint(request.getMitigationDefinition().getConstraint());
    }

    private void validateEditRequest(EditMitigationRequest request) {
        Validate.notNull(request.getMitigationDefinition(), "mitigationDefinition cannot be null.");

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

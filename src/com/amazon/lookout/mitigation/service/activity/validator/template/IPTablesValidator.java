package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.constants.DeviceNameAndScope;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;

import javax.annotation.Nonnull;

public class IPTablesValidator implements DeviceBasedServiceTemplateValidator {
    @Override
    public void validateRequestForTemplateAndDevice(@Nonnull MitigationModificationRequest request,
        @Nonnull String mitigationTemplate,
        @Nonnull DeviceNameAndScope deviceNameAndScope) {

    }

    @Override
    public void validateRequestForTemplate(@Nonnull MitigationModificationRequest request,
        @Nonnull String mitigationTemplate) {

    }

    @Override
    public void validateCoexistenceForTemplateAndDevice(@Nonnull String templateForNewDefinition,
        @Nonnull String mitigationNameForNewDefinition,
        @Nonnull MitigationDefinition newDefinition,
        @Nonnull String templateForExistingDefinition,
        @Nonnull String mitigationNameForExistingDefinition,
        @Nonnull MitigationDefinition existingDefinition) {

    }

    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Edge;
    }
}

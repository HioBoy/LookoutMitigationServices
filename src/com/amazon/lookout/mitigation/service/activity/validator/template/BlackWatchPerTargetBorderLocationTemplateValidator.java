package com.amazon.lookout.mitigation.service.activity.validator.template;
import java.util.List;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazonaws.services.s3.AmazonS3;

/**
 * This template is for BlackWatch per target mitigation on border locations.
 * @author xingbow
 *
 */
public class BlackWatchPerTargetBorderLocationTemplateValidator extends BlackWatchPerTargetMitigationTemplateValidator {
    private BlackWatchBorderLocationValidator blackWatchBorderLocationValidator;
    
    public BlackWatchPerTargetBorderLocationTemplateValidator(
            AmazonS3 blackWatchConfigS3Client, BlackWatchBorderLocationValidator BlackWatchBorderLocationValidator) {
        super(blackWatchConfigS3Client);
        this.blackWatchBorderLocationValidator = BlackWatchBorderLocationValidator;
    }

    @Override
    protected void validateLocation(List<String> locations) {
        Validate.notEmpty(locations, "locations can not be empty");
        Validate.isTrue(locations.size() == 1, String.format("locations %s should have exactly one location.", locations));
        blackWatchBorderLocationValidator.validateLocations(locations);
    }
    
    @Override
    public String getServiceNameToValidate() {
        return ServiceName.AWS;
    }
}


package com.amazon.lookout.mitigation.service.activity.validator.template;
import java.util.List;

import org.apache.commons.lang.Validate;

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
    protected void validateLocation(final String location) {
        Validate.notEmpty(location, "location can not be empty");
        blackWatchBorderLocationValidator.validateLocation(location);
    }
}


package com.amazon.lookout.mitigation.service.activity.validator.template;

import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.lang.Validate;

public class VantaLocationTemplateValidator extends BlackWatchPerTargetMitigationTemplateValidator {

    protected void validateLocation(final String location) {
        Validate.notEmpty(location, "location can not be empty");
    }
}

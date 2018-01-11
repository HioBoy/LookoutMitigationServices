package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;

import org.apache.commons.lang.Validate;

import com.amazonaws.services.s3.AmazonS3;

/**
 * This template is for BlackWatch per target mitigation on edge locations.
 * @author xingbow
 *
 */
public class BlackWatchPerTargetEdgeLocationTemplateValidator extends BlackWatchPerTargetMitigationTemplateValidator {
    public BlackWatchPerTargetEdgeLocationTemplateValidator(
            BlackWatchEdgeLocationValidator blackWatchEdgeLocationValidator, AmazonS3 blackWatchConfigS3Client) {
        super(blackWatchConfigS3Client);
        this.blackWatchEdgeLocationValidator = blackWatchEdgeLocationValidator;
    }

    private final BlackWatchEdgeLocationValidator blackWatchEdgeLocationValidator;

    @Override
    protected void validateLocation(final String location) {
        Validate.notEmpty(location, "location should not be empty.");
        blackWatchEdgeLocationValidator.validateLocation(location);
    }
}


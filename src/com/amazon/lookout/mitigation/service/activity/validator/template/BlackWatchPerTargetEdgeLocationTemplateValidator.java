package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
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
    protected void validateLocation(List<String> locations) {
        Validate.notEmpty(locations, "locations should not be empty.");
        Validate.isTrue(locations.size() == 1, String.format("locations %s should only contains 1 location.", locations));

        blackWatchEdgeLocationValidator.validateLocation(locations.get(0));
    }
    
    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Edge;
    }
}

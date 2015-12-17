package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;
import com.amazonaws.services.s3.AmazonS3;

/**
 * This template is for BlackWatch per target mitigation on edge locations.
 * @author xingbow
 *
 */
public class BlackWatchPerTargetEdgeLocationTemplateValidator extends BlackWatchPerTargetMitigationTemplateValidator {
    public BlackWatchPerTargetEdgeLocationTemplateValidator(
            EdgeLocationsHelper edgeLocationsHelper, AmazonS3 blackWatchConfigS3Client) {
        super(blackWatchConfigS3Client);
        this.edgeLocationsHelper = edgeLocationsHelper;
    }

    private static final Pattern PROD_LOCATION_PATTERN = Pattern.compile(String.format("E-([A-Z0-9]+)"));
    private final EdgeLocationsHelper edgeLocationsHelper;

    @Override
    protected void validateLocation(List<String> locations) {
        Validate.notEmpty(locations, "locations can not be empty");
        Validate.isTrue(locations.size() == 1, String.format("locations %s should have exactly one location.", locations));
        String location = locations.get(0);
        Validate.notEmpty(location, "location can not be empty");
        // translate prod location style from E-MRS50 to MRS50, so it can be same style as the locations fetched from edge location helper
        Matcher prodLocationMatcher = PROD_LOCATION_PATTERN.matcher(location);
        if (prodLocationMatcher.find()) {
            location = prodLocationMatcher.group(1);
        }
        Validate.isTrue(edgeLocationsHelper.getAllClassicPOPs().contains(location),
                String.format("location %s is not a valid edge location.", location));
    }
    
    @Override
    public String getServiceNameToValidate() {
        return ServiceName.Edge;
    }
}

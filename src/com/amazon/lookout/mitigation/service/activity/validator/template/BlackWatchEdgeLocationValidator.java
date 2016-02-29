package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;

/**
 * Validate edge location for blackwatch mitigation
 * @author xingbow
 *
 */
public class BlackWatchEdgeLocationValidator implements LocationValidator {
    private final EdgeLocationsHelper edgeLocationsHelper;
    private static final Pattern PROD_LOCATION_PATTERN = Pattern.compile(String.format("E-([A-Z0-9]+)"));
    private static final Pattern EDGE_LOCATION_PATTERN = Pattern.compile(String.format("^[GE]-([A-Z0-9]+)$"));
    
    public BlackWatchEdgeLocationValidator(EdgeLocationsHelper edgeLocationsHelper) {
        this.edgeLocationsHelper = edgeLocationsHelper;
    }
    
    @Override
    public boolean isValidLocation(String location) {
        Validate.notEmpty(location, "location can not be empty");
        Validate.isTrue(EDGE_LOCATION_PATTERN.matcher(location).find(), "invalid location " + location
                + ". edge location must exactly match pattern " + EDGE_LOCATION_PATTERN.pattern());
 
        // translate prod location style from E-MRS50 to MRS50, so it can be same style as the locations fetched from edge location helper
        Matcher prodLocationMatcher = PROD_LOCATION_PATTERN.matcher(location);
        if (prodLocationMatcher.find()) {
            location = prodLocationMatcher.group(1);
        }

        return edgeLocationsHelper.getAllClassicPOPs().contains(location);
    }
}

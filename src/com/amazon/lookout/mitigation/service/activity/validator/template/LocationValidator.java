package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

public interface LocationValidator {
    /**
     * subclass implement this method to define how to validate whether a location is valid or not
     * @param location : location
     * @return true if it is valid, false if it is not
     */
    public boolean isValidLocation(String location);
    
    /**
     * Validate one location
     * @param location : location
     * @throws IllegalArgumentException if location is not valid
     */
    public default void validateLocation(String location) {
        Validate.isTrue(isValidLocation(location), String.format("location %s is not valid", location));
    }
     
    /**
     * Validate a collection of locations
     * @param locations : collection of locations
     * @param errorMessage : use error message, if it is not null when validate failed.
     *                        else, construct an error message using failed locations
     * @throws IllegalArgumentException if any one of the location is not valid
     */
    public default void validateLocations(Collection<String> locations, String errorMessage) {
        Validate.notEmpty(locations, "locations can not be empty");
        Set<String> invalidLocations = locations.stream()
                .filter(location -> !isValidLocation(location)).collect(Collectors.toSet());
        if (errorMessage == null) {
            errorMessage = String.format("locations %s are not valid", invalidLocations);
        }
        Validate.isTrue(invalidLocations.isEmpty(), errorMessage);
    }
    
    /**
     * Validate a collection of locations
     * @param locations : collection of locations
     * @throws IllegalArgumentException if any one of the location is not valid
     */
    public default void validateLocations(Collection<String> locations) {
        validateLocations(locations, null);
    }
}

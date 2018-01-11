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
     * Validate a location
     * @param location : location
     * @param errorMessage : use error message, if it is not null when validate failed.
     *                        else, construct an error message using failed location
     * @throws IllegalArgumentException if any one of the location is not valid
     */
    public default void validateLocation(final String location, String errorMessage) {
        Validate.notEmpty(location, "location can not be empty");
        Validate.isTrue(isValidLocation(location), "location is not valid");
    }
}


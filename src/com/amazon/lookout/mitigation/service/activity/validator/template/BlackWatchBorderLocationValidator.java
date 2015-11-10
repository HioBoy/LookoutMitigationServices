package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.beans.ConstructorProperties;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

/**
 * validate location or locations that appears in preDefinedLocations list,
 * or started with certain prefix, if a prefix is given.
 * location validation is case-insensitive
 * @author xingbow
 */
public class BlackWatchBorderLocationValidator {
    private final Set<String> preDefinedLocations;
    private final String allowedLocationPrefix;

    @ConstructorProperties({"preDefinedLocations", "allowedLocationPrefix"})
    public BlackWatchBorderLocationValidator(Set<String> preDefinedLocations, String allowedLocationPrefix) {
        Validate.notNull(preDefinedLocations, "predefined location list can not be null");
        Validate.notNull(allowedLocationPrefix, "allowed location prefix can not be null");
        
        Validate.isTrue((!preDefinedLocations.isEmpty()) || (!allowedLocationPrefix.isEmpty()),
                "predefined locations is empty and allowed location prefix is empty, no location will be allowed.");
        this.preDefinedLocations = preDefinedLocations.stream().map(String::toUpperCase).collect(Collectors.toSet());
        this.allowedLocationPrefix = allowedLocationPrefix.toUpperCase();
    }
    
    /**
     * Validate one location
     * @param location : location
     * @throws IllegalArgumentException if location is not valid
     */
    public void validateLocation(String location) {
        Validate.isTrue(isValidLocation(location), String.format("location %s is not valid", location));
    }
     
    /**
     * Validate a collection of locations
     * @param locations : collection of locations
     * @throws IllegalArgumentException if any one of the location is not valid
     */
    public void validateLocations(Collection<String> locations) {
        Validate.notEmpty(locations, "locations can not be empty");
        Set<String> invalidLocations = locations.stream()
                .filter(location -> !isValidLocation(location)).collect(Collectors.toSet());
        Validate.isTrue(invalidLocations.isEmpty(), String.format("locations %s are not valid", invalidLocations));
    }
    
    private boolean isValidLocation(String location) {
        Validate.notEmpty(location, "missing location");
        location = location.toUpperCase();
        return (preDefinedLocations.contains(location)) || 
            ((!allowedLocationPrefix.isEmpty()) && (location.startsWith(allowedLocationPrefix)));
    }
}

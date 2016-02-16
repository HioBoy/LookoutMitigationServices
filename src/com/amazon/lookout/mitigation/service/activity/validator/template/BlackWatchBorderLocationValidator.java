package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.beans.ConstructorProperties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;

/**
 * validate location or locations that appears in preDefinedLocations list,
 * or started with certain prefix, if a prefix is given.
 * location validation is case-insensitive
 * @author xingbow
 */
public class BlackWatchBorderLocationValidator implements LocationValidator {
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
    
    @Override
    public boolean isValidLocation(String location) {
        Validate.notEmpty(location, "missing location");
        location = location.toUpperCase();
        return (preDefinedLocations.contains(location)) || 
            ((!allowedLocationPrefix.isEmpty()) && (location.startsWith(allowedLocationPrefix)));
    }
}

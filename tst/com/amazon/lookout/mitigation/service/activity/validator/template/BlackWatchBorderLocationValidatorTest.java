package com.amazon.lookout.mitigation.service.activity.validator.template;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import static junitparams.JUnitParamsRunner.$;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.ImmutableSet;

@RunWith(JUnitParamsRunner.class)
public class BlackWatchBorderLocationValidatorTest {
    private static final Set<String> preDefinedLocations = ImmutableSet.<String>of("LA", "LB");
    private static final String AllowedLocationPrefix = "fakerouter";

    /**
     * Test invalid location
     * @param domainAndLocation
     */
    @Test(expected = IllegalArgumentException.class)
    @Parameters({
        "LC",
        "fake",
        "invalidlocation",
        ""
        })
    public void testInvalidLocation(String location) {
        BlackWatchBorderLocationValidator validator = new BlackWatchBorderLocationValidator(
                preDefinedLocations, AllowedLocationPrefix);
        validator.validateLocation(location);
        fail("Failed valid location test with parameter : " + location);
    }

    /**
     * test invalid location without allowed prefix
     */
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLocationWithoutAllowedPrefix() {
        BlackWatchBorderLocationValidator validator = new BlackWatchBorderLocationValidator(
                preDefinedLocations, "");
        validator.validateLocation("fake");
    }
 
    /**
     * Test valid location
     * @param domainAndLocation
     */
    @Test
    @Parameters({
        "Lb",
        "la",
        "LA",
        "lB",
        "FakerouteR124we3",
        "fakerouter123"
    })
    public void testValidLocation(String location) {
        try {
            BlackWatchBorderLocationValidator validator = new BlackWatchBorderLocationValidator(
                    preDefinedLocations, AllowedLocationPrefix);
            validator.validateLocation(location);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Failed invalid location test with parameter : " + location, ex);
        }
    }

    /**
     * Test invalid multiple locations, 
     */
    @Test
    public void testInvalidLocations() {
        List<String> invalidLocations = Arrays.asList("LA", "invalidLocation", "fakerouter12", "fakerouter12");
        try {
            BlackWatchBorderLocationValidator validator = new BlackWatchBorderLocationValidator(
                    preDefinedLocations, AllowedLocationPrefix);
            validator.validateLocations(invalidLocations);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("locations [invalidLocation] are not valid", ex.getMessage());
        }
    }
    
    /**
     * Test valid multiple locations, 
     */
    @Test
    public void testValidLocations() {
        List<String> invalidLocations = Arrays.asList("LA", "Lb", "fakerouter12", "FAKErouter12");
        try {
            BlackWatchBorderLocationValidator validator = new BlackWatchBorderLocationValidator(
                    preDefinedLocations, AllowedLocationPrefix);
            validator.validateLocations(invalidLocations);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Failed invalid locations test with parameter : " + invalidLocations, ex);
        }
    }

    
    /**
     * Test invalid input
     */
    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "invalidParams")
    public void testInvalidInput(Set<String> preDefinedLocations, String allowedLocationPrefix) {
        new BlackWatchBorderLocationValidator(
                    preDefinedLocations, allowedLocationPrefix);
    }
    
    public Object[] invalidParams() {
        return $(
            $(null, null),
            $(null, AllowedLocationPrefix),
            $(preDefinedLocations, null),
            $(new HashSet<String>(), "")
            );
    }

}

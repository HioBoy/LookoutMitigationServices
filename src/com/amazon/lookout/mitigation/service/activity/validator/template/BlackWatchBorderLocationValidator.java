package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.beans.ConstructorProperties;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.json.JSONObject;

import com.aws.rip.RIPHelper;
import com.aws.rip.models.exception.RegionIsInTestException;
import com.aws.rip.models.exception.RegionNotFoundException;
import com.aws.rip.models.region.Region;

import com.amazon.lookout.mitigation.service.activity.helper.LocationConfigFileHelper;
/**
 * validate if location is present in blackwatch_border_locations.json file in package
 * BlackWatchBorderLocationConfig
 */
public class BlackWatchBorderLocationValidator implements LocationValidator {
    private final String whitelistedLocationPrefix;
    private Set<String> locationsSupported;

    @ConstructorProperties({ "region", "borderLocationConfigFilePath", "allowedLocationPrefix" })
    public BlackWatchBorderLocationValidator(String region, String borderLocationConfigFilePath, String allowedLocationPrefix) {
        Validate.notEmpty(region);
        Validate.notNull(borderLocationConfigFilePath, "borderLocationConfigFilePath cannot be null");

        locationsSupported = LocationConfigFileHelper.getLocationsSupported(region, borderLocationConfigFilePath);
        whitelistedLocationPrefix = allowedLocationPrefix;
    }

    @Override
    public boolean isValidLocation(String location) {
        Validate.notEmpty(location, "missing location");
        Validate.notNull(locationsSupported, "LocationSupported Set is NULL");
        if (!whitelistedLocationPrefix.isEmpty() && location.startsWith(whitelistedLocationPrefix)) {
            return true;
        } else {
            return locationsSupported.contains(location);
        }
    }
}
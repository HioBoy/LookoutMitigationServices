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

/**
 * validate if location is present in blackwatch_border_locations.json file in package
 * BlackWatchBorderLocationConfig
 */
public class BlackWatchBorderLocationValidator implements LocationValidator {
    private static final Log LOG = LogFactory.getLog(BlackWatchBorderLocationValidator.class);
    private static final String LOCATION_KEY = "locations";
    private static final String MITIGATION_REGION_KEY = "mitigation_region";
    private Set<String> locationsSupported;

    @ConstructorProperties({ "region", "borderLocationConfigFilePath" })
    public BlackWatchBorderLocationValidator(String region, String borderLocationConfigFilePath) throws FileNotFoundException, IOException, JSONException {
        Validate.notEmpty(region);
        Validate.notNull(borderLocationConfigFilePath, "borderLocationConfigFilePath can not be null");

        JSONObject borderLocationJSON = readBorderLocationsJSONFile(borderLocationConfigFilePath);
        locationsSupported = getLocationsSupported(region, borderLocationJSON);
    }

    /**
     * Get the set of locations that local MitSvc stack support
     * @param region (us-east-1, eu-west-1)
     * @param borderLocationJSON
     * @throws JSONException 
     * @throws RegionNotFoundException 
     * @throws RegionIsInTestException 
     * @returns Set<String>
     */
    public Set<String> getLocationsSupported(String region, JSONObject borderLocationJSON) {
        Set<String> locationsSupported = new HashSet<String>();
        String location = null;
        try {
            // get Airport code from region name. i.e. us-east-1 -> IAD
            Region reg = RIPHelper.getRegion(region);
            String localRegionAirportCode = reg.getAirportCode();

            // iterate through JSONObject and find the set of locations that local MitigationService support
            Iterator<String> locations = borderLocationJSON.keys();
            while (locations.hasNext()) {
                location = locations.next();
                String fetchedAirportCode = borderLocationJSON.getJSONObject(location).getString(MITIGATION_REGION_KEY);
                if (localRegionAirportCode.equals(fetchedAirportCode)) {
                    locationsSupported.add(location.toUpperCase());
                }
            }
        } catch (JSONException ex) {
            String msg = String.format("Caught JSONException when reading key %s for value  %s", MITIGATION_REGION_KEY, location);
            LOG.error(msg, ex);
        } catch (RegionNotFoundException ex) {
            String msg = String.format("Caught RegionNotFoundException when querying region %s", region);
            LOG.error(msg, ex);
        } catch (RegionIsInTestException ex) {
            String msg = String.format("Caught RegionIsInTestException when querying regtion  %s", region);
            LOG.error(msg, ex);
        }
        return locationsSupported;
    }

    @Override
    public boolean isValidLocation(String location) {
        Validate.notEmpty(location, "missing location");
        Validate.notNull(locationsSupported, "LocationSupported Set is NULL");

        return locationsSupported.contains(location);
    }
    
    /**
     * Converts data in json file into JSONObject
     * @param borderLocationConfigFilePath - json file path
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     * @returns JSONObject
     */
    private JSONObject readBorderLocationsJSONFile(String borderLocationConfigFilePath) throws FileNotFoundException, IOException, JSONException {
        JSONObject jsonObject = null;
        BufferedReader bufferedReader = null;
        try {
            File file = new File(borderLocationConfigFilePath);
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }
            jsonObject = new JSONObject(stringBuilder.toString());

            // returns json object of key "locations"
            return jsonObject.getJSONObject(LOCATION_KEY);
        } catch (FileNotFoundException ex) {
            String msg = String.format("Caught FileNotFound exception when reading file %s",borderLocationConfigFilePath);
            LOG.error(msg, ex);
            throw ex;
        } catch (IOException ex) {
            String msg = String.format("Caught IOException when reading file %s", borderLocationConfigFilePath);
            LOG.error(msg, ex);
            throw ex;
        } catch (JSONException ex) {
            String msg = String.format("Caught JSONException when reading JSON file %s", borderLocationConfigFilePath);
            LOG.error(msg, ex);
            throw ex;
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
    }
}
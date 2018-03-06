package com.amazon.lookout.mitigation.service.activity.helper;

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

public class LocationConfigFileHelper {
    private static final Log LOG = LogFactory.getLog(LocationConfigFileHelper.class);
    private static final String LOCATION_KEY = "locations";
    private static final String MITIGATION_REGION_KEY = "mitigation_region";
    private static final String BORDER_PROD_KEY = "border_prod";

    /**
     * Get the set of mitigation regions that our stack is deployed in
     * @param locationConfigFilePath
     * @returns Set<String> regions where MitigationService is deployed
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getMitigationRegions(final String locationConfigFilePath) {
        Set<String> mitigationRegions = new HashSet<String>();
        String regionDomain = null;

        try {
            JSONObject locationJSON = readLocationsJSONFile(locationConfigFilePath);
            Validate.notNull(locationJSON);

            // iterate through JSONObject and find the mitigation service regions
            JSONObject borderProdMitSvc = locationJSON.getJSONObject(BORDER_PROD_KEY);
            Iterator<String> regionIterator = borderProdMitSvc.keys();
            while (regionIterator.hasNext()) {
                regionDomain = regionIterator.next();
                mitigationRegions.add(regionDomain.split("\\.")[0].toLowerCase());
            }
        } catch (Exception ex) {
            String msg = String.format("Caught exception when reading key %s", BORDER_PROD_KEY);
            LOG.error(msg, ex);
        }
        return mitigationRegions;
    }

    /**
     * Get the set of locations that local MitSvc stack support
     * @param region (us-east-1, eu-west-1)
     * @param locationConfigFilePath
     * @throws JSONException 
     * @throws RegionNotFoundException 
     * @throws RegionIsInTestException 
     * @returns Set<String> locations for which the given region is the mitigation region
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getLocationsSupported(
            final String region, final String locationConfigFilePath) {
        Set<String> locationsSupported = new HashSet<String>();
        String location = null;

        try {
            JSONObject borderLocationJSON = readLocationsJSONFile(locationConfigFilePath);
            Validate.notNull(borderLocationJSON);

            // get Airport code from region name. i.e. us-east-1 -> IAD
            Region reg = RIPHelper.getRegion(region);
            String localRegionAirportCode = reg.getAirportCode();

            // iterate through JSONObject and find the set of locations 
            // that local MitigationService support
            Iterator<String> locations = borderLocationJSON.keys();
            while (locations.hasNext()) {
                location = locations.next();
                String fetchedAirportCode = borderLocationJSON.getJSONObject(location)
                        .getString(MITIGATION_REGION_KEY);
                if (localRegionAirportCode.equals(fetchedAirportCode)) {
                    locationsSupported.add(location.toUpperCase());
                }
            }
        } catch (Exception ex) {
            String msg = String.format("Caught exception when querying region %s on " +
                    "config file %s", region, locationConfigFilePath);
            LOG.error(msg, ex);
        }
        return locationsSupported;
    }

    /**
     * Converts data in json file into JSONObject
     * @param locationConfigFilePath - json file path
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     * @returns JSONObject
     */
    private static JSONObject readLocationsJSONFile(final String locationConfigFilePath)
            throws FileNotFoundException, IOException, JSONException {
        JSONObject jsonObject = null;
        BufferedReader bufferedReader = null;
        try {
            File file = new File(locationConfigFilePath);
            bufferedReader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(file), StandardCharsets.UTF_8));
            
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }
            jsonObject = new JSONObject(stringBuilder.toString());

            // returns json object of key "locations"
            return jsonObject.getJSONObject(LOCATION_KEY);
        } catch (FileNotFoundException ex) {
            String msg = String.format("Caught FileNotFound exception when reading file %s",
                    locationConfigFilePath);
            LOG.error(msg, ex);
            throw ex;
        } catch (IOException ex) {
            String msg = String.format("Caught IOException when reading file %s",
                    locationConfigFilePath);
            LOG.error(msg, ex);
            throw ex;
        } catch (JSONException ex) {
            String msg = String.format("Caught JSONException when reading JSON file %s",
                    locationConfigFilePath);
            LOG.error(msg, ex);
            throw ex;
        } catch (Exception ex) {
            String msg = String.format("Caught exception when reading file %s",
                    locationConfigFilePath);
            LOG.error(msg, ex);
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (Exception ex) {
                String msg = String.format("Exception while closing BufferedReader");
                LOG.error(msg, ex);
            }
        }
        return jsonObject;
    }
}


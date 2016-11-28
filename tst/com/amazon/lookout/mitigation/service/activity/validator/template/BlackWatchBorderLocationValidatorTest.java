package com.amazon.lookout.mitigation.service.activity.validator.template;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import java.io.FileNotFoundException;
import java.io.IOException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.json.JSONException;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class BlackWatchBorderLocationValidatorTest {
    private static final String borderLocationConfigFilePath = System.getProperty("user.dir") + "/tst-data/test_location_config.json";
    private static final String region = "test-region";

    /**
     * Test valid location
     * @param location
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test
    @Parameters({
        "BR-LOC99-1",
        "LOCATION-1"
    })
    public void testValidLocation(String location) throws FileNotFoundException, IOException, JSONException {
        BlackWatchBorderLocationValidator validator = null;
        validator = new BlackWatchBorderLocationValidator("us-east-1", borderLocationConfigFilePath);
        assertTrue(validator.isValidLocation(location));
    }
    
    /**
     * Test invalid location
     * @param location
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test
    @Parameters({
        "BR-LOC990-1",
        "LOCATION-11"
    })
    public void testInvalidLocation(String location) throws FileNotFoundException, IOException, JSONException {
        BlackWatchBorderLocationValidator validator = null;
        validator = new BlackWatchBorderLocationValidator(region, borderLocationConfigFilePath);
        assertFalse(validator.isValidLocation(location));
    }
    
    /**
     * Test lowercase valid location, should fail the validation since we only allow uppercase
     * @param location
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test
    @Parameters({
        "BR-loc99-1",
        "location-1"
    })
    public void testLowercaseLocation(String location) throws FileNotFoundException, IOException, JSONException {
        BlackWatchBorderLocationValidator validator = null;
        validator = new BlackWatchBorderLocationValidator(region, borderLocationConfigFilePath);
        assertFalse(validator.isValidLocation(location));
    }

    /**
     * Test empty location
     * @param location
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test(expected=IllegalArgumentException.class)
    @Parameters({
        ""
    })
    public void testEmptyLocation(String location) throws FileNotFoundException, IOException, JSONException {
        BlackWatchBorderLocationValidator validator = null;
        validator = new BlackWatchBorderLocationValidator(region, borderLocationConfigFilePath);
        assertFalse(validator.isValidLocation(location));
    }

    /**
     * Test Invalid file path
     * @throws JSONException 
     * @throws IOException 
     * @throws FileNotFoundException 
     */
    @Test(expected=FileNotFoundException.class)
    @Parameters({
        "LOCATION-1"
    })
    public void testInvalidConfigFilePath(String location) throws FileNotFoundException, IOException, JSONException {
        new BlackWatchBorderLocationValidator(region, "/random/path/file.json");
    }
}
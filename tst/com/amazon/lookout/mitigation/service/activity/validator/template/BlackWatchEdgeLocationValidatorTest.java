package com.amazon.lookout.mitigation.service.activity.validator.template;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.Before;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

import com.amazon.lookout.mitigation.service.workflow.helper.EdgeLocationsHelper;

@RunWith(JUnitParamsRunner.class)
public class BlackWatchEdgeLocationValidatorTest {
    private static final String whitelistedLocationPrefix = "testPrefix";
    private static final Set<String> preDefinedLocations  = new HashSet<String>(Arrays.asList("LOADTEST-118", "LOADTEST-119"));

    @Mock
    private EdgeLocationsHelper edgeLocationsHelper;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        doReturn(new HashSet<String>(Arrays.asList("AMS1", "NRT54", "MIA3-C1", "G-IAD55")))
                .when(edgeLocationsHelper).getAllClassicPOPs();
    }

    /**
     * Test prod.xml regular expression
     *
     * @param location
     *
     */
    @Test
    @Parameters({
            "E-AMS1",
            "E-NRT54",
            "E-MIA3-C1"
    })
    public void testProdRegex(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^E-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertTrue(validator.isValidLocation(location));
    }

    /**
     * Test gamma.xml regular expression
     *
     * @param location
     *
     */
    @Test
    @Parameters({
            "G-IAD55"
    })
    public void testGammaRegex(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^fakerouter-?\\d+$|^G-([A-Z0-9]+)$", new HashSet<String>(), "");
        assertTrue(validator.isValidLocation(location));
    }

    /**
     * Test valid location
     *
     * @param location
     *
     */
    @Test
    @Parameters({
            "E-AMS1",
            "E-NRT54",
            "E-MIA3-C1",
            "G-IAD55"
    })
    public void testValidLocation(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertTrue(validator.isValidLocation(location));
    }

    /**
     * Test valid predefined location
     *
     * @param location
     *
     */
    @Test
    @Parameters({
            "LOADTEST-118",
            "LOADTEST-119"
    })
    public void testValidPredefinedLocation(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", preDefinedLocations, "");
        assertTrue(validator.isValidLocation(location));
    }

    /**
     * Test valid location prefix
     *
     * @param location
     *
     */
    @Test
    @Parameters({
            "testPrefix",
            "testPrefix54-"
    })
    public void testValidLocationPrefix(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), whitelistedLocationPrefix);
        assertTrue(validator.isValidLocation(location));
    }

    /**
     * Test invalid location 1 : not in the list of POPs returned
     *
     * @param location
     *
     */
    @Test
    @Parameters({
            "E-AMS54"
    })
    public void testInvalidLocation1(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertFalse(validator.isValidLocation(location));
    }

    /**
     * Test invalid location 2: does not match regular expression
     *
     * @param location
     *
     */
    @Test(expected=IllegalArgumentException.class)
    @Parameters({
            "E-AMS54-",
            "E-MIA3-C1X"
    })
    public void testInvalidLocation2(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertFalse(validator.isValidLocation(location));
    }

    /**
     * Test lowercase valid location, should fail the validation since we only allow uppercase
     *
     * @param location
     *
     */
    @Test(expected=IllegalArgumentException.class)
    @Parameters({
            "E-Nrt54",
            "E-Mia3-C1",
            "E-MIA3-c11"
    })
    public void testLowercaseLocation(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertFalse(validator.isValidLocation(location));
    }

    /**
     * Test empty location prefix
     *
     * @param location
     *
     */
    @Test(expected=IllegalArgumentException.class)
    @Parameters({
            "testPrefix",
            "testPrefix54-"
    })
    public void testEmptyLocationPrefix(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertFalse(validator.isValidLocation(location));
    }

    /**
     * Test empty location
     *
     * @param location
     *
     */
    @Test(expected=IllegalArgumentException.class)
    @Parameters({
            ""
    })
    public void testEmptyLocation(String location) {
        BlackWatchEdgeLocationValidator validator = new BlackWatchEdgeLocationValidator(edgeLocationsHelper, "^[GE]-([A-Z0-9]+)(-C[0-9]+)?$", new HashSet<String>(), "");
        assertFalse(validator.isValidLocation(location));
    }
}
package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import amazon.mws.data.Datapoint;
import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.blackwatch.location.state.model.LocationOperation;
import com.amazon.lookout.mitigation.ActiveMitigationsHelper;
import com.amazon.lookout.mitigation.exception.ExternalDependencyException;
import com.amazon.lookout.mitigation.service.activity.helper.mws.MWSRequestException;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.blackwatch.location.state.storage.LocationStateDynamoDBHelper;
import com.amazon.lookout.mitigation.location.type.LocationType;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazon.lookout.mitigation.service.activity.helper.mws.MWSHelper;

public class DDBBasedLocationStateInfoHandler implements LocationStateInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedLocationStateInfoHandler.class);
    public static final String DDB_QUERY_FAILURE_COUNT = "DynamoDBQueryFailureCount";

    private static final int DATAPOINTS_TO_BE_EVALUATED = 2; //MWS Datapoints that needs to be evaluated

    private final int totalSegments = 2;

    private LocationStateDynamoDBHelper locationStateDynamoDBHelper;

    private ActiveMitigationsHelper activeMitigationsHelper;

    private MWSHelper mwsHelper;

    private final Pattern locationNameRegex = Pattern.compile("^(?<location>[A-Za-z]+-[A-Za-z]+[0-9]+(?:(?:-[fF][0-9]+-)|(?:-[dD])|(?:-)))(?<stack>[0-9]+)$");

    public DDBBasedLocationStateInfoHandler(@NonNull LocationStateDynamoDBHelper locationStateDynamoDBHelper) {
        this.locationStateDynamoDBHelper = locationStateDynamoDBHelper;
        this.activeMitigationsHelper = null;
        this.mwsHelper = null;
    }

    @ConstructorProperties({ "locationStateDynamoDBHelper", "activeMitigationsHelper", "mwsHelper"})
    public DDBBasedLocationStateInfoHandler(@NonNull LocationStateDynamoDBHelper locationStateDynamoDBHelper,
                                            @NonNull ActiveMitigationsHelper activeMitigationsHelper,
                                            @NonNull MWSHelper mwsHelper) {
        this.locationStateDynamoDBHelper = locationStateDynamoDBHelper;
        this.activeMitigationsHelper = activeMitigationsHelper;
        this.mwsHelper = mwsHelper;
    }

    @Override
    public LocationState getLocationState(String location, TSDMetrics tsdMetrics) {
        Validate.notEmpty(location);
        Validate.notNull(tsdMetrics);

        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.getLocationState")) {
            try {
                return locationStateDynamoDBHelper.getLocationState(location);
            } catch (Exception ex) {
                String msg = String.format("Caught Exception when querying with LocationName - %s", location);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
        }
    }

    public BlackWatchLocation convertLocationState(LocationState in) {
        String locationBuildStatus = null;

        if (in.getBuildStatus() == null) {
            String msg = String.format("Location build status not found for location. " + in.getLocationName()
                + " You probably have to delete the location from location_state table in mitigation service account.");
            LOG.warn(msg);
        } else {
            locationBuildStatus = in.getBuildStatus().name();
        }

        BlackWatchLocation out = new BlackWatchLocation();
        out.setLocation(in.getLocationName());
        out.setAdminIn(in.getAdminIn());
        out.setInService(in.getInService());
        out.setChangeUser(in.getChangeUser());
        out.setChangeTime(Optional.of(in).map(LocationState::getChangeTime).orElse(0L));
        out.setChangeReason(in.getChangeReason());
        out.setActiveBGPSpeakerHosts(Optional.of(in).map(LocationState::getActiveBGPSpeakerHosts).orElse(0));
        out.setActiveBlackWatchHosts(Optional.of(in).map(LocationState::getActiveBlackWatchHosts).orElse(0));
        out.setLocationType(in.getLocationType());
        out.setBuildStatus(locationBuildStatus);
        return out;
    }

    /**
     * Generate a List with BlackWatchLocation objects.
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public List<BlackWatchLocation> getAllBlackWatchLocations(TSDMetrics tsdMetrics) {
        Validate.notNull(tsdMetrics);

        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.getAllBlackWatchLocations")) {
            List<BlackWatchLocation> listOfBlackWatchLocation = new ArrayList<>();
            try {
                List<LocationState> locationsWithAdminIn = locationStateDynamoDBHelper.getAllLocationStates(totalSegments);
                for (LocationState locationState : locationsWithAdminIn) {
                   listOfBlackWatchLocation.add(convertLocationState(locationState));
                }
            } catch (Exception ex) {
                String msg = String.format("Caught Exception when scaning for the Location State");
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
            return listOfBlackWatchLocation;
        }
    }

    private void updateLocationState(LocationState locationState, TSDMetrics tsdMetrics) {
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.updateLocationState")) {
            locationStateDynamoDBHelper.updateLocationState(locationState);
        } catch (ConditionalCheckFailedException conditionEx) {
            String msg = String.format("Caught Condition Check Exception updating location: %s with state: %s",
                    locationState.getLocationName(), locationState);
            LOG.warn(msg, conditionEx);
            tsdMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
            throw conditionEx;
        } catch (Exception ex) {
            String msg = String.format("Caught Exception updating location: %s with state: %s",
                    locationState.getLocationName(), locationState);
            LOG.warn(msg, ex);
            tsdMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
            throw ex;
        }
    }

    private Map<String, LocationOperation> computeOperationLocks(LocationState locationState, boolean requestAdminIn,
                                                                 String operationId, String changeId, boolean overrideLocks) {
        if (locationState == null) {
            String msg = "Invalid request! - location is not found in " + locationStateDynamoDBHelper.getLocationStateTableName() + " and "
                    + "LocationType is not specified.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }

        Map<String, LocationOperation> operationLocks = locationState.getOrCreateOperationLocksMap();
        LOG.info("Location: " + locationState.getLocationName() + "AdminIn = \"" + requestAdminIn + "\"  requested for changeId: " + changeId);

        if (requestAdminIn) {
            if (overrideLocks) {
                LOG.info("Force adminIn operation. Clearing OperationLocks.");
                operationLocks.clear();
            }
            operationLocks.remove(operationId);
        } else {
            LocationOperation locationOperation = new LocationOperation();
            locationOperation.setChangeId(changeId);
            locationOperation.setTimestamp(System.currentTimeMillis());
            operationLocks.putIfAbsent(operationId, locationOperation);
        }
        return operationLocks;
    }

    /**
     * Update AdminIn state given a location and reason.
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public void updateBlackWatchLocationAdminIn(String location, boolean requestAdminIn, String reason,
                                                String locationType, String operationId, String changeId,
                                                boolean overrideLocks, TSDMetrics tsdMetrics) {
        Validate.notNull(tsdMetrics);
        Validate.notEmpty(location);
        Validate.notEmpty(reason);

        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.updateBlackWatchLocationAdminIn")) {
            LocationState ls = getLocationState(location, subMetrics);

            Map<String, LocationOperation> operationLocks = computeOperationLocks(ls, requestAdminIn, operationId, changeId, overrideLocks);
            boolean adminIn = operationLocks.isEmpty();

            LocationType setType = null;
            if (locationType != null && locationType.trim().length() != 0) {
                try {
                    setType = LocationType.valueOf(locationType);
                }
                catch (IllegalArgumentException argumentException) {
                    String msg = "Invalid Location type found! " + argumentException.getMessage();
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
            }

            if (adminIn == true) {
                if (setType == null) {
                    if (ls == null) {
                        String msg = "Invalid request! - " + location + " is not found in " +  locationStateDynamoDBHelper.getLocationStateTableName() + " and "
                                + "LocationType is not specified.";
                        LOG.info(msg);
                        throw new IllegalStateException(msg);
                    }
                    else if (ls.getLocationType().equalsIgnoreCase(LocationType.UNKNOWN.toString())) {
                        String msg = "LocationType for " + location + " is UNKNOWN. So AdminIn cannot be updated to True if LocationType is UNKNOWN.";
                        LOG.info(msg);
                        throw new IllegalStateException(msg);
                    }
                } else if (setType == LocationType.UNKNOWN) {
                    String msg = "Invalid Location type found! LocationType cannot take UNKNOWN if AdminIn is set to True";
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
            }

            if (ls == null) {
                ls = LocationState.builder().locationName(location).build();
            }

            ls.setAdminIn(adminIn);
            ls.setChangeReason(reason);
            ls.setChangeTime(System.currentTimeMillis());
            ls.setOperationLocks(operationLocks);

            if (setType != null) {
                ls.setLocationType(locationType);
                ls.setHasLocationTypeOverride(true);
            }

            updateLocationState(ls, subMetrics);
        }
    }

    public LocationState requestHostStatusChange(String location, String hostName, HostStatusEnum requestedStatus,
                                                 String changeReason, String changeUser, String changeHost,
                                                 List<String> relatedLinks, boolean createMissingHost,
                                                 TSDMetrics tsdMetrics) {
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.requestHostStatusChange")) {
            LocationState locationState = getLocationState(location, tsdMetrics);

            if (locationState.requestHostStatusChange(hostName, requestedStatus, changeReason, changeUser, changeHost,
                    relatedLinks, createMissingHost)) {
                updateLocationState(locationState, subMetrics);
            }

            return locationState;
        }
    }

    public List<String> getPendingOperationLocks(String location, TSDMetrics tsdMetrics) {
        LocationState locationState = getLocationState(location, tsdMetrics);
        Map<String, LocationOperation> operationLocks = locationState.getOrCreateOperationLocksMap();
        List<String> pendingOperationLocks = new ArrayList<>();
        operationLocks.values().forEach(locationOperation -> pendingOperationLocks.add(locationOperation.getChangeId()));
        return pendingOperationLocks;
    }

    /**
     * Method to get mapping operationId -> changeId
     * @param location String
     * @param tsdMetrics A TSDMetrics Object
     *
     */
    @Override
    public Map<String, String> getOperationChanges(String location, TSDMetrics tsdMetrics) {
        LOG.info("Getting operation changes for location: " + location);
        LocationState locationState = getLocationState(location, tsdMetrics);
        Map<String, LocationOperation> operationLocks = locationState.getOrCreateOperationLocksMap();
        return operationLocks.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().getChangeId()
                ));
    }

    /**
     * This function builds and returns the location pair name of a location. It doesn't check if the pair
     * exists, it just builds the name based on the established naming conventions
     *
     * @param locationName name of a border location who's pair matches the form: br-abc12-1
     * @return the paired stack name, with the stack name replaced by the pair, in the example above, br-abc12-2
     */
    String locationPairName(@NonNull String locationName) {
        Matcher locationNameMatcher = locationNameRegex.matcher(locationName);

        if (!locationNameMatcher.matches()) {
            LOG.warn(String.format("Location '%s' is not in the right format", locationName));
            return null;
        }

        String locationNameNoStack = locationNameMatcher.group("location");
        int stackNumber;
        try {
            stackNumber = Integer.parseInt(locationNameMatcher.group("stack"));
        } catch (NumberFormatException ex) {
            LOG.warn(String.format("Location '%s' is not in the right format", locationName));
            return null;
        }

        if (stackNumber <= 0) {
            LOG.warn("Location stack number must be greater than 0 for location " + locationName);
            return null;
        }

        int pairStackNum;
        if (stackNumber % 2 != 0) {
            /* If stack number is odd, then the pair is the next highest stack number (1 -> 2) */
            pairStackNum = stackNumber + 1;
        } else {
            /* If stack number is even, then the pair is the next lower stack number (2 -> 1) */
            pairStackNum = stackNumber - 1;
        }

        return String.format("%s%d", locationNameNoStack, pairStackNum);
    }

    /**
     * This function returns all locations protecting the same network. This logic can be adjusted over time if
     * future BlackWatch locations have more complex protection schemes.
     * @param currentLocation a LocationState that we use to build a list of BlackWatchLocation objects that protect the same network.
     * @param tsdMetrics a TSDMetrics object
     * @return A list of BlackWatchLocations that protect the same network as currentLocation
     *         including a BlackWatchLocation representing currentLocation as well.
     */
    List<BlackWatchLocation> getAllBlackWatchLocationsProtectingTheSameNetwork(LocationState currentLocation, TSDMetrics tsdMetrics) {
        boolean locTypeStartsWithTc = Optional.of(currentLocation)
                .map(LocationState::getLocationType)
                .map(locType -> locType.startsWith("TC"))
                .orElse(false);

        if (locTypeStartsWithTc) {
            /* Only border/transit center locations protect resources in pairs */
            String locationPairName = locationPairName(currentLocation.getLocationName());
            LocationState pairLocationState = getLocationState(locationPairName, tsdMetrics);

            if (pairLocationState != null) {
                return Arrays.asList(convertLocationState(currentLocation), convertLocationState(pairLocationState));
            } else {
                LOG.warn(String.format("Could not find location pair of '%s'", currentLocation.getLocationName()));
            }
        }

        return Collections.singletonList(convertLocationState(currentLocation));
    }

    private boolean hasAnnouncedRoutes(String location, TSDMetrics tsdMetrics) {
        double routeCount = 0;
        try {
            List<Datapoint> datapoints = mwsHelper.getBGPTotalAnnouncements(location, tsdMetrics);
            if (datapoints.size() < DATAPOINTS_TO_BE_EVALUATED) {
                throw new IllegalStateException("Last two datapoints for the location: " + location + " not found");
            }
            datapoints = datapoints.subList(datapoints.size() - DATAPOINTS_TO_BE_EVALUATED, datapoints.size());
            for (Datapoint datapoint : datapoints) {
                routeCount += datapoint.getValue();
            }
        } catch (MWSRequestException e) {
            return true;
        }
        return routeCount != 0;
    }

    public boolean evaluateOperationalFlags(LocationState locationState, boolean areRoutesAnnounced, boolean hasExpectedMitigations) {
        boolean isOperational;
        /* A weird safe check where conditions to consider stack operational changes on AdminIn value
         * Stack Operational when taking InService -> stack operational when hasExpectedMitigations, InService and routesAnnounced are true
         * Stack Operational when taking Out of Service ->  If any of InService and routesAnnounced is true
         * */
        if(locationState.getAdminIn()){
            isOperational = hasExpectedMitigations && locationState.getInService() && areRoutesAnnounced;
        } else {
            //when we admin out a location
            //the isOperation flag will become false only when all conditions are false
            isOperational = locationState.getInService() || areRoutesAnnounced;
        }
        LOG.info("Location: " + locationState.getLocationName() + "\n\nFlags:\n AdminIn: " + locationState.getAdminIn()
                + "\n InService: " + locationState.getInService() + "\n hasExpectedMitigations: " + hasExpectedMitigations
                + "\n areRoutesAnnounced: " + areRoutesAnnounced + "\n isOperational: " + isOperational);
        return isOperational;
    }

    private boolean isLocationOperational(String location, List<BlackWatchLocation> allStacksAtLocation, TSDMetrics tsdMetrics)
            throws ExternalDependencyException {
        LocationState locationState = getLocationState(location, tsdMetrics);
        boolean areRoutesAnnounced = hasAnnouncedRoutes(location, tsdMetrics);
        boolean hasExpectedMitigations = activeMitigationsHelper.hasExpectedMitigations(DeviceName.BLACKWATCH_BORDER, allStacksAtLocation, location);
        return evaluateOperationalFlags(locationState, areRoutesAnnounced, hasExpectedMitigations);
    }

    @Override
    public boolean validateOtherStacksInService(String location, TSDMetrics tsdMetrics) throws ExternalDependencyException {
        LOG.info("Validating if other stacks are in service for location: " + location);
        LocationState locationState = getLocationState(location, tsdMetrics);
        List<BlackWatchLocation> allStacksAtLocation = getAllBlackWatchLocationsProtectingTheSameNetwork(locationState, tsdMetrics);
        boolean otherStackinService = allStacksAtLocation.size() > 1 ? true : false;
        LOG.info("Stacks at this location: " + allStacksAtLocation.toString());
        for (BlackWatchLocation stack : allStacksAtLocation) {
            /*
             * This is intermediate check when request to take a stack out arrives while other stack
             * is being taken out (other stack: AdminIn = false, but InService true).
             */
            if(!stack.getLocation().equals(location)) {
                if ( !stack.isAdminIn()) {
                    return false;
                }
                otherStackinService = otherStackinService 
                        && isLocationOperational(stack.getLocation(), allStacksAtLocation, tsdMetrics);
            }
        }
        return otherStackinService;
    }

    @Override
    public boolean checkIfLocationIsOperational(String location, TSDMetrics tsdMetrics) throws ExternalDependencyException {
        LOG.info("Checking if location: " + location + ", is operational.");
        LocationState locationState = getLocationState(location, tsdMetrics);
        if (location.toLowerCase().startsWith("be")) {
            return locationState.getInService();
        }
        else {
            List<BlackWatchLocation> allStacksAtLocation = getAllBlackWatchLocationsProtectingTheSameNetwork(locationState, tsdMetrics);
            return isLocationOperational(location, allStacksAtLocation, tsdMetrics); }
    }
}

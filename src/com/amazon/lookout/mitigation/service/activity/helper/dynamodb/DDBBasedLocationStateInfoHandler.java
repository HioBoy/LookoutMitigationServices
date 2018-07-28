package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.List;

import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.blackwatch.location.state.model.LocationType;
import com.amazon.blackwatch.location.state.storage.LocationStateDynamoDBHelper;
import com.amazon.lookout.mitigation.service.BlackWatchLocation;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazon.lookout.mitigation.service.mitigation.model.LocationBuildStatus;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

public class DDBBasedLocationStateInfoHandler implements LocationStateInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedLocationStateInfoHandler.class);
    public static final String DDB_QUERY_FAILURE_COUNT = "DynamoDBQueryFailureCount";

    private final int totalSegments = 2;

    private LocationStateDynamoDBHelper locationStateDynamoDBHelper;

    @ConstructorProperties({ "locationStateDynamoDBHelper" })
    public DDBBasedLocationStateInfoHandler(@NonNull LocationStateDynamoDBHelper locationStateDynamoDBHelper) {
        this.locationStateDynamoDBHelper = locationStateDynamoDBHelper;
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
        BlackWatchLocation out = new BlackWatchLocation();
        out.setLocation(in.getLocationName());
        out.setAdminIn(in.getAdminIn());
        out.setInService(in.getInService());
        out.setChangeUser(in.getChangeUser());
        out.setChangeTime(in.getChangeTime());
        out.setChangeReason(in.getChangeReason());
        out.setActiveBGPSpeakerHosts(in.getActiveBGPSpeakerHosts());
        out.setActiveBlackWatchHosts(in.getActiveBlackWatchHosts());
        out.setLocationType(in.getLocationType());
        out.setBuildStatus(in.getBuildStatus().name());
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
                for (LocationState ls : locationsWithAdminIn) {
                   listOfBlackWatchLocation.add(convertLocationState(ls));
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

    /**
     * Update AdminIn state given a location and reason.
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public void updateBlackWatchLocationAdminIn(String location, boolean adminIn, String reason, String locationType, TSDMetrics tsdMetrics) {
        Validate.notNull(tsdMetrics);
        Validate.notEmpty(location);
        Validate.notEmpty(reason);

        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.updateBlackWatchLocationAdminIn")) {
            LocationState ls = getLocationState(location, subMetrics);

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
            if (setType != null) {
                ls.setLocationType(locationType);
            }

            updateLocationState(ls, subMetrics);
        }
    }

    public LocationState requestHostStatusChange(String location, String hostName, HostStatusEnum requestedStatus,
                                                 String changeReason, String changeUser, String changeHost,
                                                 List<String> relatedLinks, TSDMetrics tsdMetrics) {
        try (TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.requestHostStatusChange")) {
            LocationState locationState = getLocationState(location, tsdMetrics);

            if (locationState.requestHostStatusChange(hostName, requestedStatus, changeReason, changeUser, changeHost, relatedLinks)) {
                updateLocationState(locationState, subMetrics);
            }

            return locationState;
        }
    }
}
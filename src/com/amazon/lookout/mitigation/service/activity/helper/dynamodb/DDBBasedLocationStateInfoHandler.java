package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Generate a List with BlackWatchLocation objects.
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public List<BlackWatchLocation> getBlackWatchLocation(String region, TSDMetrics tsdMetrics) {
        Validate.notNull(tsdMetrics);

        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.getBlackWatchLocation")) {
            List<BlackWatchLocation> listOfBlackWatchLocation = new ArrayList<>();
            BlackWatchLocation blackWatchLocations;
            try {
                List<LocationState> locationsWithAdminIn = locationStateDynamoDBHelper.getAllLocationStates(totalSegments);
                for (LocationState ls : locationsWithAdminIn) {
                    blackWatchLocations = new BlackWatchLocation();
                    blackWatchLocations.setLocation(ls.getLocationName());
                    blackWatchLocations.setAdminIn(ls.getAdminIn());
                    blackWatchLocations.setInService(ls.getInService());
                    blackWatchLocations.setChangeUser(ls.getChangeUser());
                    blackWatchLocations.setChangeTime(ls.getChangeTime());
                    blackWatchLocations.setChangeReason(ls.getChangeReason());

                    listOfBlackWatchLocation.add(blackWatchLocations);
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
            /* saveExpression for conditionalUpdate, update AdminIn state only if locationName(hashkey) exists in the table */

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
                    try {
                        LocationState ls = locationStateDynamoDBHelper.getLocationState(location);
                        if (ls == null) {
                            String msg = "Invalid request! - " + location + " is not found in " +  locationStateDynamoDBHelper.getLocationStateTableName() + " and "
                                    + "LocationType is not specified.";
                            LOG.info(msg);
                            throw new IllegalStateException(msg);
                        }
                        else {
                            if (ls.getLocationType().equalsIgnoreCase(LocationType.UNKNOWN.toString())) {
                                String msg = "LocationType for " + location + " is UNKNOWN. So AdminIn cannot be updated to True if LocationType is UNKNOWN.";
                                LOG.info(msg);
                                throw new IllegalStateException(msg);
                            }
                        }
                    } catch (Exception ex) {
                        String msg = String.format("Caught Exception when querying with LocationName - %s", location);
                        LOG.warn(msg, ex);
                        subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                        throw ex;
                    }
                }
                else if (setType == LocationType.UNKNOWN) {
                    String msg = "Invalid Location type found! LocationType cannot take UNKNOWN if AdminIn is set to True";
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
            }

            LocationState ls = LocationState.builder()
                    .locationName(location)
                    .adminIn(adminIn)
                    .changeReason(reason)
                    .changeTime(System.currentTimeMillis())
                    .locationType(locationType)
                    .build();

            try {
                locationStateDynamoDBHelper.updateLocationState(ls);
            } catch (ConditionalCheckFailedException conditionEx) {
                String msg = String.format("Caught Condition Check Exception when updating AdminIn state to %s in location: "
                        + "%s with reason: %s and LocationType: %s", adminIn, location, reason, (locationType == null) ? "" : locationType);
                LOG.warn(msg, conditionEx);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw conditionEx;
            } catch (Exception ex) {
                String msg = String.format("Caught Exception when updating AdminIn state to %s in location: %s with reason: %s "
                        + "and LocationType: %s", adminIn, location, reason, (locationType == null) ? "" : locationType);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
        }
    }
}
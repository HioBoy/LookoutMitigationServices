package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.List;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.location.state.model.LocationState;
import com.amazon.blackwatch.location.state.storage.LocationStateDynamoDBHelper;
import com.amazon.coral.metrics.MetricsFactory;
import com.amazon.coral.metrics.NullMetricsFactory;
import com.amazon.lookout.mitigation.service.BlackWatchLocations;
import com.amazon.lookout.mitigation.service.activity.helper.LocationStateInfoHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

public class DDBBasedLocationStateInfoHandler implements LocationStateInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedLocationStateInfoHandler.class);
    public static final String DDB_QUERY_FAILURE_COUNT = "DynamoDBQueryFailureCount";

    private final AmazonDynamoDBClient dynamoDBClient;
    private final DynamoDB dynamoDB;
    private final int totalSegments = 2;
    
    private final String domain;
    private final String realm;
    
    private LocationStateDynamoDBHelper locationStateDynamoDBHelper;

    public DDBBasedLocationStateInfoHandler(@NonNull AmazonDynamoDBClient dynamoDBClient, @NonNull String domain, @NonNull String realm) {
        this.dynamoDBClient = dynamoDBClient;
        this.dynamoDB = new DynamoDB(dynamoDBClient);
        this.realm = realm.toUpperCase();
        this.domain = domain.toUpperCase();
    }

    /**
     * Generate a List with BlackWatchLocations objects.
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public List<BlackWatchLocations> getBlackWatchLocations(TSDMetrics tsdMetrics) {
        Validate.notNull(tsdMetrics);
        MetricsFactory metricsFactory = new NullMetricsFactory(); // delete me TODO
        locationStateDynamoDBHelper = new LocationStateDynamoDBHelper(dynamoDBClient, realm, domain, 5L, 5L, metricsFactory);
        
        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedLocationStateInfoHandler.getBlackWatchLocations")) {
            List<LocationState> locationsWithAdminIn = locationStateDynamoDBHelper.getAllLocationStates(totalSegments);
            List<BlackWatchLocations> listOfBlackWatchLocations = new ArrayList<>();
            try {
                for(int i = 0; i < locationsWithAdminIn.size(); i++) {
                    BlackWatchLocations blackWatchLocations = new BlackWatchLocations();
                    blackWatchLocations.setLocation(locationsWithAdminIn.get(i).getLocationName());
                    blackWatchLocations.setAdminIn(locationsWithAdminIn.get(i).isAdminIn());
                    
                    listOfBlackWatchLocations.add(blackWatchLocations);
                }
            } catch (Exception ex) {
                String msg = String.format("Caught Exception when scaning for the Location State");
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
            return listOfBlackWatchLocations;
        }
    }
}
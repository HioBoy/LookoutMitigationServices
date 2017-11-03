package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazon.blackwatch.host.status.model.HostStatusEnum;
import com.amazon.blackwatch.location.state.model.LocationHostStatus;
import com.amazon.blackwatch.location.state.model.LocationState;
import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.blackwatch.host.status.model.HostStatus;
import com.amazon.lookout.mitigation.service.HostStatusInLocation;
import com.amazon.lookout.mitigation.service.activity.helper.HostStatusInfoHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class DDBBasedHostStatusInfoHandler implements HostStatusInfoHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedHostStatusInfoHandler.class);
    public static final String DDB_QUERY_FAILURE_COUNT = "DynamoDBQueryFailureCount";

    private final AmazonDynamoDB dynamoDBClient;
    private final DynamoDB dynamoDB;

    private final String hostStatusTableName;
    private final Table table;

    public DDBBasedHostStatusInfoHandler(@NonNull AmazonDynamoDB dynamoDBClient, @NonNull String domain, @NonNull String realm) {
        this.dynamoDBClient = dynamoDBClient;
        this.dynamoDB = new DynamoDB(dynamoDBClient);
        this.hostStatusTableName = String.format(HostStatus.TABLE_NAME_FORMAT, realm.toUpperCase(), domain.toUpperCase());
        this.table = dynamoDB.getTable(this.hostStatusTableName);
    }

    private static String hostStatusEnumToString(HostStatusEnum in) {
        return in != null ? in.name() : null;
    }

    /**
     * Generate a Map with HostName as Key and its IS_ACTIVE boolean status as value for a given location.
     * @param locationState : location state object
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public List<HostStatusInLocation> getHostsStatus(LocationState locationState, TSDMetrics tsdMetrics) {
        Validate.notNull(locationState);
        Validate.notNull(tsdMetrics);

        String location = locationState.getLocationName();

        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedHostStatusInfoHandler.getHostsStatus")) {
            QuerySpec querySpec = new QuerySpec().withHashKey(HostStatus.HOST_LOCATION_KEY, location);

            List<HostStatusInLocation> listOfHostStatusInLocations = new ArrayList<>();
            try {
                ItemCollection<QueryOutcome> items = table.query(querySpec);
                for (Item item : items) {
                    HostStatusInLocation hostStatusinLocation = new HostStatusInLocation();
                    String hostName = item.getString(HostStatus.HOST_NAME_KEY);
                    hostStatusinLocation.setHostName(hostName);
                    hostStatusinLocation.setIsActive(item.getBOOL(HostStatus.IS_ACTIVE_KEY));
                    hostStatusinLocation.setLatestHeartbeatTimestamp(item.getLong(HostStatus.LATEST_HEART_BEAT_TIMESTAMP_KEY));
                    hostStatusinLocation.setHostType(item.getString(HostStatus.HOST_TYPE_KEY));
                    hostStatusinLocation.setDeviceName(item.getString(HostStatus.DEVICE_NAME_KEY));
                    hostStatusinLocation.setHardwareType(item.getString(HostStatus.HARDWARE_TYPE_KEY));
                    LocationHostStatus locationHostStatus = locationState.getOrCreateHosts().get(hostName);
                    if (locationHostStatus != null) {
                        hostStatusinLocation.setCurrentStatus(hostStatusEnumToString(locationHostStatus.getCurrentStatus()));
                        hostStatusinLocation.setCurrentStatusChangeTime(locationHostStatus.getCurrentStatusChangeTime());
                        hostStatusinLocation.setNextStatus(hostStatusEnumToString(locationHostStatus.getNextStatus()));
                        hostStatusinLocation.setNextStatusChangeTime(locationHostStatus.getNextStatusChangeTime());
                        hostStatusinLocation.setRequestedStatus(hostStatusEnumToString(locationHostStatus.getRequestedStatus()));
                        hostStatusinLocation.setRequestedStatusChangeTime(locationHostStatus.getRequestedStatusChangeTime());
                        hostStatusinLocation.setRequestedStatusCompletionTime(locationHostStatus.getRequestedStatusCompletionTime());
                        hostStatusinLocation.setChangeReason(locationHostStatus.getChangeReason());
                        hostStatusinLocation.setChangeUser(locationHostStatus.getChangeUser());
                        hostStatusinLocation.setChangeHost(locationHostStatus.getChangeHost());
                        hostStatusinLocation.setRelatedLinks(locationHostStatus.getOrCreateRelatedLinks().stream().collect(Collectors.toList()));
                    }
                    listOfHostStatusInLocations.add(hostStatusinLocation);
                }
            } catch (Exception ex) {
                String msg = String.format("Caught Exception when querying for the host status associated with location : %s", location);
                LOG.warn(msg, ex);
                subMetrics.addOne(DDB_QUERY_FAILURE_COUNT);
                throw ex;
            }
            return listOfHostStatusInLocations;
        }
    }
}

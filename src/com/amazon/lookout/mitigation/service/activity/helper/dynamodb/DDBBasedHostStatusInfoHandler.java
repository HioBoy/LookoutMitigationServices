package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.List;

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

    /**
     * Generate a Map with HostName as Key and its IS_ACTIVE boolean status as value for a given location.
     * @param location : location
     * @param tsdMetrics A TSDMetrics object.
     */
    @Override
    public List<HostStatusInLocation> getHostsStatus(String location, TSDMetrics tsdMetrics) {
        Validate.notEmpty(location);
        Validate.notNull(tsdMetrics);

        try(TSDMetrics subMetrics = tsdMetrics.newSubMetrics("DDBBasedHostStatusInfoHandler.getHostsStatus")) {
            QuerySpec querySpec = new QuerySpec().withHashKey(HostStatus.HOST_LOCATION_KEY, location);

            List<HostStatusInLocation> listOfHostStatusInLocations = new ArrayList<>();
            try {
                ItemCollection<QueryOutcome> items = table.query(querySpec);
                for (Item item : items) {
                    HostStatusInLocation hostStatusinLocation = new HostStatusInLocation();
                    hostStatusinLocation.setHostName(item.getString(HostStatus.HOST_NAME_KEY));
                    hostStatusinLocation.setIsActive(item.getBOOL(HostStatus.IS_ACTIVE_KEY));
                    hostStatusinLocation.setLatestHeartbeatTimestamp(item.getLong(HostStatus.LATEST_HEART_BEAT_TIMESTAMP_KEY));
                    hostStatusinLocation.setHostType(item.getString(HostStatus.HOST_TYPE_KEY));
                    hostStatusinLocation.setDeviceName(item.getString(HostStatus.DEVICE_NAME_KEY));
                    hostStatusinLocation.setHardwareType(item.getString(HostStatus.HARDWARE_TYPE_KEY));
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

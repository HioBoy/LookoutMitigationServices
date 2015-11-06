package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Set;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import lombok.NonNull;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.request.RollbackMitigationRequestInternal;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

public class DDBBasedRollbackRequestStorageHandler extends DDBBasedRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedRollbackRequestStorageHandler.class);
    private static final String FAILED_TO_STORE_ROLLBACK_REQUEST_KEY = "FailedToStoreRollbackRequest";
    
    public DDBBasedRollbackRequestStorageHandler(AmazonDynamoDBClient dynamoDBClient, String domain) {
        super(dynamoDBClient, domain);
    }

    /**
     * Store the rollback request to request table
     */
    @Override
    public long storeRequestForWorkflow(@NonNull MitigationModificationRequest request, Set<String> locations, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(locations);
        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRollbackRequestStorageHandler.storeRequestForWorkflow")) {
            try {
                subMetrics.addZero(FAILED_TO_STORE_ROLLBACK_REQUEST_KEY); 

                if (!(request instanceof RollbackMitigationRequestInternal)) {
                    subMetrics.addOne(INVALID_REQUEST_TO_STORE_KEY);
                    String msg = "Requested RollbackRequestStorageHandler to store a non-rollback request";
                    LOG.error(msg + ", for request: " + request + " in locations: " + locations);
                    throw new IllegalArgumentException(msg);
                }
                
                RollbackMitigationRequestInternal rollbackRequest = (RollbackMitigationRequestInternal) request;
                return storeUpdateMitigationRequest(request, rollbackRequest.getMitigationDefinition(),
                        rollbackRequest.getMitigationVersion(), RequestType.RollbackRequest, locations, metrics);
            } catch(Exception ex) {
                subMetrics.addOne(FAILED_TO_STORE_ROLLBACK_REQUEST_KEY); 
                throw ex;
            }
        }
    }
}

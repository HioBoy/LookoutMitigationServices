package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Set;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

public class DDBBasedEditRequestStorageHandler extends DDBBasedCreateAndEditRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedEditRequestStorageHandler.class);
    
    private static final String FAILED_TO_STORE_EDIT_REQUEST_KEY = "FailedToStoreEditRequest";
    

    public DDBBasedEditRequestStorageHandler(AmazonDynamoDB dynamoDBClient, String domain, @NonNull TemplateBasedRequestValidator templateBasedRequestValidator) {
        super(dynamoDBClient, domain, templateBasedRequestValidator);
    }

    /**
     * Stores the edit request into the DDB Table.
     * @param request Request to be stored.
     * @param locations Set of String representing the locations where this request applies.
     * @param metrics
     * @return RequestStorageResponse which includes the workflowId, and new mitigation version that this request was stored with, using the algorithm above.
     */
    @Override
    public RequestStorageResponse storeRequestForWorkflow(@NonNull MitigationModificationRequest request, Set<String> locations, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(locations);

        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedEditRequestStorageHandler.storeRequestForWorkflow")) {
            try {
                subMetrics.addZero(FAILED_TO_STORE_EDIT_REQUEST_KEY);

                if (!(request instanceof EditMitigationRequest)) {
                    subMetrics.addOne(INVALID_REQUEST_TO_STORE_KEY);
                    String msg = "Requested EditRequestStorageHandler to store a non-edit request";
                    LOG.error(msg + ", for request: " + request + " in locations: " + locations);
                    throw new IllegalArgumentException(msg);
                }
                EditMitigationRequest editMitigationRequest = (EditMitigationRequest) request;
                
                return storeRequestForWorkflow(
                       RequestType.EditRequest, request, locations, editMitigationRequest.getMitigationDefinition(), 
                       editMitigationRequest.getMitigationVersion(), UPDATE_REQUEST_STORAGE_FAILED_LOGSCAN_TOKEN, 
                       subMetrics);
            } catch (Exception ex) {
                subMetrics.addOne(FAILED_TO_STORE_EDIT_REQUEST_KEY);
                throw ex;
            }
        }
    }
}

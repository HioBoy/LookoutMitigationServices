package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import lombok.NonNull;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageResponse;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

/**
 * DDBBasedCreateRequestStorageHandler is responsible for persisting create requests into DDB.
 * As part of persisting this request, based on the existing requests in the table, this handler also determines the workflowId 
 * to be used for the current request.
 */
@ThreadSafe
public class DDBBasedCreateRequestStorageHandler extends DDBBasedCreateAndEditRequestStorageHandler implements RequestStorageHandler {
    private static final Log LOG = LogFactory.getLog(DDBBasedCreateRequestStorageHandler.class);

    // Prefix tags for logging warnings to be monitored.
    private static final String CREATE_REQUEST_STORAGE_FAILED_LOG_PREFIX = "[CREATE_REQUEST_STORAGE_FAILED]";


    public DDBBasedCreateRequestStorageHandler(AmazonDynamoDB dynamoDBClient, String domain, @NonNull TemplateBasedRequestValidator templateBasedRequestValidator) {
        super(dynamoDBClient, domain, templateBasedRequestValidator);
    }

    /**
     * Stores the create request into the DDB Table. While storing, it identifies the new workflowId to be associated with this request and returns back the same.
     * The algorithm it uses to identify the workflowId to use is:
     * 1. Identify the deviceName that corresponds to this request.
     * 2. Query for all active mitigations for this device.
     * 3. If no active mitigations exist for this device, start with 1.
     * 4. Else identify the max workflowId for the active mitigations for this device and try to use this maxWorkflowId+1 as the new workflowId
     *    when storing the request.
     * 5. If when storing the request we encounter an exception, it could be either because someone else started using the maxWorkflowId+1 workflowId for that device
     *    or it is some transient exception. In either case, we query the DDB table once again for mitigations >= maxWorkflowId for the device and
     *    continue with step 4. 
     * @param request Request to be stored.
     * @param locations Set of String representing the locations where this request applies.
     * @param metrics
     * @return RequestStorageResponse include workflowsId and new mitigation version, that this request was stored with, using the algorithm above.
     */
    @Override
    public RequestStorageResponse storeRequestForWorkflow(@NonNull MitigationModificationRequest request, @NonNull Set<String> locations, @NonNull TSDMetrics metrics) {
        Validate.notEmpty(locations);
        
        CreateMitigationRequest createMitigationRequest = (CreateMitigationRequest) request;

        try (TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedCreateRequestStorageHandler.storeRequestForWorkflow")) {
            MitigationDefinition mitigationDefinition = createMitigationRequest.getMitigationDefinition();

            return storeRequestForWorkflow(
                    RequestType.CreateRequest, createMitigationRequest, locations,  mitigationDefinition, 
                    INITIAL_MITIGATION_VERSION, CREATE_REQUEST_STORAGE_FAILED_LOG_PREFIX, subMetrics); 
        }
    }
}

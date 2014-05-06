package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.EnumMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.aws158.commons.metric.TSDMetrics;
import com.amazon.coral.google.common.collect.ImmutableMap;
import com.amazon.coral.google.common.collect.Maps;
import com.amazon.lookout.mitigation.service.InternalServerError500;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageHandler;
import com.amazon.lookout.mitigation.service.activity.helper.RequestStorageManager;
import com.amazon.lookout.mitigation.service.activity.validator.template.TemplateBasedRequestValidator;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

/**
 * This RequestStorageManager is responsible for storing requests in DynamoDB.
 * This manager delegates the actual task of storage to a RequestStorageHandler based on the RequestType.
 *
 */
@ThreadSafe
public class DDBBasedRequestStorageManager implements RequestStorageManager {
    private static final Log LOG = LogFactory.getLog(DDBBasedRequestStorageManager.class);
    
    private static final String NO_STORAGE_HANDLE_FOUND_LOG_TAG = "[NO_REQUST_STORAGE_HANDLER]";
    
    private static final String REQUEST_TYPE_PROPERTY_KEY = "RequestType";

    private final ImmutableMap<RequestType, RequestStorageHandler> requestTypeToStorageHandler;
    
    public DDBBasedRequestStorageManager(@Nonnull AmazonDynamoDBClient dynamoDBClient, @Nonnull String domain, 
                                         @Nonnull TemplateBasedRequestValidator templateBasedValidator) {
        Validate.notNull(dynamoDBClient);
        Validate.notEmpty(domain);
        Validate.notNull(templateBasedValidator);
        
        requestTypeToStorageHandler = Maps.immutableEnumMap(getRequestTypeToStorageHandlerMap(dynamoDBClient, domain, templateBasedValidator));
    }
    
    /**
     * Returns an EnumMap with RequestType as the key and the RequestStorageHandler with the responsibility of storing corresponding request.
     * @param dynamoDBClient
     * @param domain Domain where this service runs, we have separate DDB tables for beta/gamma/prod, domain helps us identify the correct tableName.
     * @param templateBasedValidator TemplateBasedValidator which might be required by some RequestStorageHandlers to perform checks before storage.
     * @return EnumMap with RequestType as the key and the corresponding RequestStorageHandler responsible for storing this request.
     */
    private EnumMap<RequestType, RequestStorageHandler> getRequestTypeToStorageHandlerMap(AmazonDynamoDBClient dynamoDBClient, String domain, 
                                                                                          TemplateBasedRequestValidator templateBasedValidator) {
        EnumMap<RequestType, RequestStorageHandler> requestStorageHandlerMap = new EnumMap<RequestType, RequestStorageHandler>(RequestType.class);
        DDBBasedCreateRequestStorageHandler createStorageHandler = new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        requestStorageHandlerMap.put(RequestType.CreateRequest, createStorageHandler);
        
        DDBBasedDeleteRequestStorageHandler deleteStorageHandler = new DDBBasedDeleteRequestStorageHandler(dynamoDBClient, domain);
        requestStorageHandlerMap.put(RequestType.DeleteRequest, deleteStorageHandler);
        
        return requestStorageHandlerMap;
    }
    
    /**
     * Stores the request that requires starting a new workflow into DDB and returns the corresponding workflowId to this workflow.
     * @param request Request that needs to be stored.
     * @param requestType Type of request (eg: Create/Edit/Delete).
     * @param metrics
     * @return WorkflowId to be used to for processing this request, which was determined by the RequestStorageHandler based on the workflowId we recorded this request with.
     */
    @Override
    public long storeRequestForWorkflow(@Nonnull MitigationModificationRequest request, @Nonnull RequestType requestType, @Nonnull TSDMetrics metrics) {
        Validate.notNull(request);
        Validate.notNull(requestType);
        Validate.notNull(metrics);
        
        TSDMetrics subMetrics = metrics.newSubMetrics("DDBBasedRequestStorageManager.storeRequestForWorkflow");
        try {
            subMetrics.addProperty(REQUEST_TYPE_PROPERTY_KEY, requestType.name());
            RequestStorageHandler storageHandler = getRequestStorageHandler(requestType);
            return storageHandler.storeRequestForWorkflow(request, subMetrics);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Responsible for recording the SWFRunId corresponding to the workflow request just created in DDB.
     * @param deviceName DeviceName corresponding to the workflow being run.
     * @param workflowId WorkflowId for the workflow being run.
     * @param runId SWF assigned runId for the running instance of this workflow.
     * @param requestType Instance of the RequestType enum, useful for delegating the update to the RequestStorageHandler corresponding to this request.
     * @param metrics
     */
    public void updateRunIdForWorkflowRequest(@Nonnull String deviceName, long workflowId, @Nonnull String runId, 
                                              @Nonnull RequestType requestType, @Nonnull TSDMetrics metrics) {
        Validate.notEmpty(deviceName);
        Validate.isTrue(workflowId > 0);
        Validate.notEmpty(runId);
        Validate.notNull(requestType);
        Validate.notNull(metrics);
        
        RequestStorageHandler handler = getRequestStorageHandler(requestType);
        handler.updateRunIdForWorkflowRequest(deviceName, workflowId, runId, metrics);
    }
    
    /**
     * Helper to get the RequestStorageHandler corresponding to the request type. Protected for unit-tests.
     * @param requestType
     * @return RequestStorageHandler instance corresponding to the requestType passed as input.
     */
    protected RequestStorageHandler getRequestStorageHandler(RequestType requestType) {
        RequestStorageHandler storageHandler = requestTypeToStorageHandler.get(requestType);
        if (storageHandler == null) {
            String msg = NO_STORAGE_HANDLE_FOUND_LOG_TAG + " - No RequestStorageHandler found for requestType: " + requestType.name();
            LOG.warn(msg);
            throw new InternalServerError500(msg);
        }
        return storageHandler;
    }

}

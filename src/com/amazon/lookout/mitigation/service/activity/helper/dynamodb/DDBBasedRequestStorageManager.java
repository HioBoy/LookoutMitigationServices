package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.EnumMap;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
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
import com.amazon.lookout.mitigation.service.constants.RequestType;
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
        DDBBasedCreateRequestStorageHandler createStorageHandler = getCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
        requestStorageHandlerMap.put(RequestType.CreateRequest, createStorageHandler);
        
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
            if (storageHandler == null) {
                String msg = NO_STORAGE_HANDLE_FOUND_LOG_TAG + " - No RequestStorageHandler found for request: " + requestType.name() +
                             ", Request: " + ReflectionToStringBuilder.toString(request);
                LOG.warn(msg);
                throw new InternalServerError500(msg);
            }
            
            return storageHandler.storeRequestForWorkflow(request, subMetrics);
        } finally {
            subMetrics.end();
        }
    }
    
    /**
     * Helper method to create an instance of DDBBasedCreateRequestStorageHandler.
     * @param dynamoDBClient
     * @param domain
     * @param templateBasedValidator
     * @return New instance of DDBBasedCreateRequestStorageHandler
     */
    private DDBBasedCreateRequestStorageHandler getCreateRequestStorageHandler(AmazonDynamoDBClient dynamoDBClient, String domain, 
                                                                               TemplateBasedRequestValidator templateBasedValidator) {
        return new DDBBasedCreateRequestStorageHandler(dynamoDBClient, domain, templateBasedValidator);
    }
    
    /**
     * Helper to get the RequestStorageHandler corresponding to the request type. Protected for unit-tests.
     * @param requestType
     * @return RequestStorageHandler instance corresponding to the requestType passed as input.
     */
    protected RequestStorageHandler getRequestStorageHandler(RequestType requestType) {
        return requestTypeToStorageHandler.get(requestType);
    }

}

package com.amazon.lookout.mitigation.service.activity.helper.dynamodb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.NonNull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.util.CollectionUtils;

import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithLocations;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.simpleworkflow.flow.DataConverter;
import com.amazonaws.services.simpleworkflow.flow.JsonDataConverter;
import com.google.common.collect.ImmutableSet;

import static com.amazon.lookout.ddb.model.MitigationRequestsModel.*;

public class DDBRequestSerializer {
    private static final DataConverter jsonDataConverter = new JsonDataConverter();
    
    /**
     * Generates the Map of String - representing the attributeName, to AttributeValue - representing the value to store for this attribute.
     * Protected for unit-testing. 
     * @param request Request to be persisted.
     * @param mitigationDefinition : mitigation definition to be stored, can be null.
     * @param locations Set of String where this request applies.
     * @param deviceName DeviceName corresponding to this request.
     * @param workflowId WorkflowId to store this request with.
     * @param requestType Type of request (eg: create/edit/delete).
     * @param mitigationVersion Version number to use for storing the mitigation in this request.
     * @return Map of String (attributeName) -> AttributeValue.
     */
    public static Map<String, AttributeValue> serializeRequest(
            @NonNull MitigationModificationRequest request,
            MitigationDefinition mitigationDefinition, 
            @NonNull Set<String> locations,
            @NonNull DeviceName deviceName, long workflowId,
            @NonNull RequestType requestType, int mitigationVersion, long updateWorkflowId)
    {
        Validate.notEmpty(locations);
        
        Map<String, AttributeValue> attributesInItemToStore = new HashMap<>();
        
        AttributeValue attributeValue = new AttributeValue(deviceName.name());
        attributesInItemToStore.put(DEVICE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(workflowId));
        attributesInItemToStore.put(WORKFLOW_ID_KEY, attributeValue);
        
        // TODO remove remove remove
        // Only needed for migration to scopeless mitigation service
        attributesInItemToStore.put("DeviceScope", new AttributeValue("GLOBAL"));

        attributeValue = new AttributeValue(WorkflowStatus.RUNNING);
        attributesInItemToStore.put(WORKFLOW_STATUS_KEY, attributeValue);
        
        attributeValue = new AttributeValue(request.getMitigationName());
        attributesInItemToStore.put(MITIGATION_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue().withN(String.valueOf(mitigationVersion));
        attributesInItemToStore.put(MITIGATION_VERSION_KEY, attributeValue);
        
        attributeValue = new AttributeValue(request.getMitigationTemplate());
        attributesInItemToStore.put(MITIGATION_TEMPLATE_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(request.getServiceName());
        attributesInItemToStore.put(SERVICE_NAME_KEY, attributeValue);
        
        DateTime now = new DateTime(DateTimeZone.UTC);
        attributeValue = new AttributeValue().withN(String.valueOf(now.getMillis()));
        attributesInItemToStore.put(REQUEST_DATE_IN_MILLIS_KEY, attributeValue);
        
        attributeValue = new AttributeValue(requestType.name());
        attributesInItemToStore.put(REQUEST_TYPE_KEY, attributeValue);
        
        MitigationActionMetadata metadata = request.getMitigationActionMetadata();
        attributeValue = new AttributeValue(metadata.getUser());
        attributesInItemToStore.put(USER_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(metadata.getToolName());
        attributesInItemToStore.put(TOOL_NAME_KEY, attributeValue);
        
        attributeValue = new AttributeValue(metadata.getDescription());
        attributesInItemToStore.put(USER_DESCRIPTION_KEY, attributeValue);
        
        // Related tickets isn't a required attribute, hence checking if it has been provided before creating a corresponding AttributeValue for it.
        if ((metadata.getRelatedTickets() != null) && !metadata.getRelatedTickets().isEmpty()) {
            attributeValue = new AttributeValue().withSS(metadata.getRelatedTickets());
            attributesInItemToStore.put(RELATED_TICKETS_KEY, attributeValue);
        }
        
        attributeValue = new AttributeValue().withSS(locations);
        attributesInItemToStore.put(LOCATIONS_KEY, attributeValue);
        
        // Specifying MitigationDefinition only makes sense for the some of the requests and hence extracting it only for such requests.
        if (mitigationDefinition != null) {
            String mitigationDefinitionJSONString = jsonDataConverter.toData(mitigationDefinition); 
            attributeValue = new AttributeValue(mitigationDefinitionJSONString);
            attributesInItemToStore.put(MITIGATION_DEFINITION_KEY, attributeValue);
            
            int mitigationDefinitionHashCode = mitigationDefinitionJSONString.hashCode();
            attributeValue = new AttributeValue().withN(String.valueOf(mitigationDefinitionHashCode));
            attributesInItemToStore.put(MITIGATION_DEFINITION_HASH_KEY, attributeValue);
        }
        
        List<MitigationDeploymentCheck> deploymentChecks = request.getPreDeploymentChecks();
        if ((deploymentChecks == null) || deploymentChecks.isEmpty()) {
            attributeValue = new AttributeValue().withN(String.valueOf(0));
            attributesInItemToStore.put(NUM_PRE_DEPLOY_CHECKS_KEY, attributeValue);
        } else {
            attributeValue = new AttributeValue().withN(String.valueOf(deploymentChecks.size()));
            attributesInItemToStore.put(NUM_PRE_DEPLOY_CHECKS_KEY, attributeValue);
            
            String preDeployChecksAsJSONString = jsonDataConverter.toData(deploymentChecks);
            attributeValue = new AttributeValue(preDeployChecksAsJSONString);
            attributesInItemToStore.put(PRE_DEPLOY_CHECKS_DEFINITION_KEY, attributeValue);
        }
        
        deploymentChecks = request.getPostDeploymentChecks();
        if ((deploymentChecks == null) || deploymentChecks.isEmpty()) {
            attributeValue = new AttributeValue().withN(String.valueOf(0));
            attributesInItemToStore.put(NUM_POST_DEPLOY_CHECKS_KEY, attributeValue);
        } else {
            attributeValue = new AttributeValue().withN(String.valueOf(deploymentChecks.size()));
            attributesInItemToStore.put(NUM_POST_DEPLOY_CHECKS_KEY, attributeValue);
            
            String postDeployChecksAsJSONString = jsonDataConverter.toData(deploymentChecks);
            attributeValue = new AttributeValue(postDeployChecksAsJSONString);
            attributesInItemToStore.put(POST_DEPLOY_CHECKS_DEFINITION_KEY, attributeValue);
        }
        
        attributeValue = new AttributeValue().withN(String.valueOf(updateWorkflowId));
        attributesInItemToStore.put(UPDATE_WORKFLOW_ID_KEY, attributeValue);

        //Abort Deployment Flag, default to false
        attributeValue = new AttributeValue().withBOOL(false);
        attributesInItemToStore.put(ABORT_FLAG_KEY, attributeValue);
        
        return attributesInItemToStore;
    }
    
    /**
     * Helper method to create the expected conditions when inserting record into the MitigationRequests table.
     * The conditions to check for is to ensure we don't already have a record with the same hashKey (deviceName) and rangeKey (workflowId).
     * @return Map of String (attribute name) to ExpectedAttributeValue to represent the requirement of non-existence of the keys of this map.
     */
    public static Map<String, ExpectedAttributeValue> expectedConditionsForNewRecord() {
        Map<String, ExpectedAttributeValue> expectedCondition = new HashMap<>();
        
        ExpectedAttributeValue notExistsCondition = new ExpectedAttributeValue(false);
        expectedCondition.put(DEVICE_NAME_KEY, notExistsCondition);
        expectedCondition.put(WORKFLOW_ID_KEY, notExistsCondition);
        
        return expectedCondition;
    }
    
    public static Map<String, AttributeValue> getKey(@NonNull String deviceName, long workflowId) {
        Validate.notEmpty(deviceName);
        Map<String, AttributeValue> key = new HashMap<>();
        key.put(DEVICE_NAME_KEY, new AttributeValue(deviceName));
        key.put(WORKFLOW_ID_KEY, new AttributeValue().withN(String.valueOf(workflowId)));
        return key;
    }
    
    public static PrimaryKey getPrimaryKey(@NonNull String deviceName, long workflowId) {
        Validate.notEmpty(deviceName);
        return new PrimaryKey().addComponents(
                new KeyAttribute(DEVICE_NAME_KEY, deviceName),
                new KeyAttribute(WORKFLOW_ID_KEY, workflowId));
    }
    
    private static List<AttributeValue> attributeListOf(int value) {
        return Collections.singletonList(new AttributeValue().withN(String.valueOf(value)));
    }
    
    private static List<AttributeValue> attributeListOf(String value) {
        return Collections.singletonList(new AttributeValue().withS(String.valueOf(value)));
    }
    
    /**
     * Get the query key for a given device. This can be used on the main table or on any LSI.
     * 
     * @param deviceName
     * @return
     */
    public static Map<String, Condition> getQueryKeyForDevice(@NonNull String deviceName) {
        Validate.notEmpty(deviceName);
        
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put(DEVICE_NAME_KEY, new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(Collections.singleton(new AttributeValue(deviceName))));
        
        return keyConditions;
    }
    
    /**
     * Generate the keys for querying mitigations for the device passed as input and whose workflowIds are equal or greater than the workflowId passed as input.
     * Protected for unit-testing. This should be used on the table, not on an index.
     * 
     * @param deviceName Device for whom we need to find the active mitigations.
     * @param minWorkflowId WorkflowId which we need to constraint our query by. 
     *  We should be querying for existing mitigations whose workflowIds are >= this value.
     * @return Keys for querying mitigations for the device passed as input with workflowIds >= the workflowId passed as input above.
     */
    public static Map<String, Condition> getPrimaryQueryKey(@NonNull String deviceName, Long minWorkflowId) {
        Validate.notEmpty(deviceName);
        
        Map<String, Condition> keyConditions = getQueryKeyForDevice(deviceName);

        if (minWorkflowId != null) {
            keyConditions.put(WORKFLOW_ID_KEY, new Condition()
                .withComparisonOperator(ComparisonOperator.GE)
                .withAttributeValueList(Collections.singleton(new AttributeValue().withN(String.valueOf(minWorkflowId)))));
        }

        return keyConditions;
    }
    
    /**
     * Generate the keys for querying active mitigations for the device passed as input. This should be used
     * with the UNEDITED_MITIGATIONS_LSI_NAME index.
     * 
     * An active mitigation is one whose UpdateWorkflowId column hasn't been updated in the MitigationRequests table.
     * @param deviceName Device for whom we need to find the active mitigations.
     * @return Keys for querying active mitigations for the device passed as input.
     */
    public static Map<String, Condition> getQueryKeyForActiveMitigationsForDevice(@NonNull String deviceName) {
        Validate.notEmpty(deviceName);
        
        Map<String, Condition> keyConditions = getQueryKeyForDevice(deviceName);
        addActiveRequestCondition(keyConditions);

        return keyConditions;
    }
    
    /**
     * Generate the keys for querying active mitigations for the device passed as input. This should be used
     * with the MITIGATION_NAME_LSI index.
     * 
     * An active mitigation is one whose UpdateWorkflowId column hasn't been updated in the MitigationRequests table.
     * @param deviceName Device for whom we need to find the active mitigations.
     * @return Keys for querying active mitigations for the device passed as input.
     */
    public static Map<String, Condition> getQueryKeyForMitigationName(@NonNull String deviceName, @NonNull String mitigationName) {
        Validate.notEmpty(deviceName);
        Validate.notEmpty(mitigationName);
        
        Map<String, Condition> keyConditions = getQueryKeyForDevice(deviceName);
        keyConditions.put(MITIGATION_NAME_KEY, new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(attributeListOf(mitigationName)));

        return keyConditions;
    }
    
    public static void addActiveRequestCondition(@NonNull Map<String, Condition> conditions) {
        conditions.put(UPDATE_WORKFLOW_ID_KEY, new Condition()
            .withComparisonOperator(ComparisonOperator.EQ)
            .withAttributeValueList(attributeListOf(UPDATE_WORKFLOW_ID_FOR_UNEDITED_REQUESTS)));
    }
    
    public static void addNotFailedCondition(@NonNull Map<String, Condition> conditions) {
        conditions.put(WORKFLOW_STATUS_KEY, new Condition()
            .withComparisonOperator(ComparisonOperator.NE)
            .withAttributeValueList(attributeListOf(WorkflowStatus.FAILED)));
    }
    
    public static void addMinVersionCondition(@NonNull Map<String, Condition> conditions, int minVersion) {
        conditions.put(MITIGATION_VERSION_KEY, new Condition()
            .withComparisonOperator(ComparisonOperator.GE)
            .withAttributeValueList(attributeListOf(minVersion)));
    }
    
    /**
     * Generate an instance of MitigationRequestDescription from a Map of String to AttributeValue.
     * @param keyValues Map of String to AttributeValue.
     * @return A MitigationRequestDescription object.
     */
    public static MitigationRequestDescription convertToRequestDescription(Map<String, AttributeValue> keyValues) {
        if (keyValues == null) {
            return null;
        }
        
        MitigationRequestDescription mitigationDescription = new MitigationRequestDescription();
        
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setUser(keyValues.get(USER_NAME_KEY).getS());
        mitigationActionMetadata.setDescription(keyValues.get(USER_DESCRIPTION_KEY).getS());
        mitigationActionMetadata.setToolName(keyValues.get(TOOL_NAME_KEY).getS());
        
        // Related tickets is optional, hence the check.
        if (keyValues.containsKey(RELATED_TICKETS_KEY)) {
            List<String> tickets = keyValues.get(RELATED_TICKETS_KEY).getSS();
            if (!CollectionUtils.isEmpty(tickets)) {
                mitigationActionMetadata.setRelatedTickets(tickets);
            }
        }
        mitigationDescription.setMitigationActionMetadata(mitigationActionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        if (keyValues.containsKey(MITIGATION_DEFINITION_KEY) && !StringUtils.isEmpty(keyValues.get(MITIGATION_DEFINITION_KEY).getS())) {
            mitigationDefinition = jsonDataConverter.fromData(keyValues.get(MITIGATION_DEFINITION_KEY).getS(), MitigationDefinition.class);
        }
        mitigationDescription.setMitigationDefinition(mitigationDefinition);
        
        mitigationDescription.setMitigationTemplate(keyValues.get(MITIGATION_TEMPLATE_NAME_KEY).getS());
        mitigationDescription.setDeviceName(keyValues.get(DEVICE_NAME_KEY).getS());
        mitigationDescription.setJobId(Long.parseLong(keyValues.get(WORKFLOW_ID_KEY).getN()));
        mitigationDescription.setMitigationName(keyValues.get(MITIGATION_NAME_KEY).getS());
        mitigationDescription.setMitigationVersion(Integer.parseInt(keyValues.get(MITIGATION_VERSION_KEY).getN()));
        mitigationDescription.setRequestDate(Long.parseLong(keyValues.get(REQUEST_DATE_IN_MILLIS_KEY).getN()));
        mitigationDescription.setRequestStatus(keyValues.get(WORKFLOW_STATUS_KEY).getS());
        mitigationDescription.setRequestType(keyValues.get(REQUEST_TYPE_KEY).getS());
        mitigationDescription.setServiceName(keyValues.get(SERVICE_NAME_KEY).getS());
        mitigationDescription.setUpdateJobId(Long.parseLong(keyValues.get(UPDATE_WORKFLOW_ID_KEY).getN()));
        
        return mitigationDescription;
    }
    
    public static MitigationRequestDescriptionWithLocations convertToRequestDescriptionWithLocations(
            @NonNull Map<String, AttributeValue> item) 
    {
        MitigationRequestDescriptionWithLocations descriptionWithLocations = new MitigationRequestDescriptionWithLocations();
        descriptionWithLocations.setMitigationRequestDescription(convertToRequestDescription(item));
        descriptionWithLocations.setLocations(item.get(LOCATIONS_KEY).getSS());
        return descriptionWithLocations;
    }
    
    //get the abort flag from DDB item. 
    public static Boolean getRequestAbortFlag(
            @NonNull Item item) 
    {
        return item.getBoolean(ABORT_FLAG_KEY);
    }
    
    public static MitigationRequestDescriptionWithLocations convertToRequestDescriptionWithLocations(@NonNull Item item) {
        MitigationRequestDescriptionWithLocations descriptionWithLocations = new MitigationRequestDescriptionWithLocations();
        descriptionWithLocations.setMitigationRequestDescription(convertToRequestDescription(item));
        descriptionWithLocations.setLocations(new ArrayList<>(item.getStringSet(LOCATIONS_KEY)));
        return descriptionWithLocations;
    }
    
    /*
     * Same as above function, handle new Dynamodb query return type Item
     */
    public static MitigationRequestDescription convertToRequestDescription(Item item) {
        if (item == null) {
            return null;
        }
        
        MitigationRequestDescription mitigationDescription = new MitigationRequestDescription();
        
        MitigationActionMetadata mitigationActionMetadata = new MitigationActionMetadata();
        mitigationActionMetadata.setUser(item.getString(USER_NAME_KEY));
        mitigationActionMetadata.setDescription(item.getString(USER_DESCRIPTION_KEY));
        mitigationActionMetadata.setToolName(item.getString(TOOL_NAME_KEY));
        
        // Related tickets is optional, hence the check.
        Set<String> tickets = item.getStringSet(RELATED_TICKETS_KEY);
        if (!CollectionUtils.isEmpty(tickets)) {
            mitigationActionMetadata.setRelatedTickets(
                    new ArrayList<String>(tickets));
        }
        mitigationDescription.setMitigationActionMetadata(mitigationActionMetadata);

        MitigationDefinition mitigationDefinition = new MitigationDefinition();
        if (!StringUtils.isEmpty(item.getString(MITIGATION_DEFINITION_KEY))) {
            mitigationDefinition = jsonDataConverter.fromData(item.getString(MITIGATION_DEFINITION_KEY), MitigationDefinition.class);
        }
        mitigationDescription.setMitigationDefinition(mitigationDefinition);
        
        mitigationDescription.setMitigationTemplate(item.getString(MITIGATION_TEMPLATE_NAME_KEY));
        mitigationDescription.setDeviceName(item.getString(DEVICE_NAME_KEY));
        mitigationDescription.setJobId(item.getLong(WORKFLOW_ID_KEY));
        mitigationDescription.setMitigationName(item.getString(MITIGATION_NAME_KEY));
        mitigationDescription.setMitigationVersion(item.getInt(MITIGATION_VERSION_KEY));
        mitigationDescription.setRequestDate(item.getLong(REQUEST_DATE_IN_MILLIS_KEY));
        mitigationDescription.setRequestStatus(item.getString(WORKFLOW_STATUS_KEY));
        mitigationDescription.setRequestType(item.getString(REQUEST_TYPE_KEY));
        mitigationDescription.setServiceName(item.getString(SERVICE_NAME_KEY));
        mitigationDescription.setUpdateJobId(item.getLong(UPDATE_WORKFLOW_ID_KEY));
        
        return mitigationDescription;
    }
    
    /**
     * A class that stores a minimal summary of a request to use for validating new edit/rollback, etc
     * requests.
     * 
     * @author stevenso
     *
     */
    @Data
    public static class RequestSummary {
        private static ImmutableSet<String> requiredAttributes = 
                ImmutableSet.of(
                        MITIGATION_VERSION_KEY, REQUEST_TYPE_KEY, MITIGATION_TEMPLATE_NAME_KEY, MITIGATION_NAME_KEY,
                        WORKFLOW_ID_KEY, WORKFLOW_STATUS_KEY);
        
        public static ImmutableSet<String> getRequiredAttributes() {
            return requiredAttributes;
        }
        
        private final String mitigationName;
        private final int mitigationVersion;
        private final String requestType;
        private final String mitigationTemplate;
        private final long workflowId; 
        private final String workflowStatus;
        
        public RequestSummary(@NonNull Item item) {
            mitigationName = item.getString(MITIGATION_NAME_KEY);
            mitigationVersion = item.getInt(MITIGATION_VERSION_KEY);
            requestType = item.getString(REQUEST_TYPE_KEY);
            mitigationTemplate = item.getString(MITIGATION_TEMPLATE_NAME_KEY);
            workflowId = item.getLong(WORKFLOW_ID_KEY);
            workflowStatus = item.getString(WORKFLOW_STATUS_KEY);
        }
        
        public RequestSummary(@NonNull Map<String, AttributeValue> item) {
            mitigationName = item.get(MITIGATION_NAME_KEY).getS();
            mitigationVersion = Integer.parseInt(item.get(MITIGATION_VERSION_KEY).getN());
            requestType = item.get(REQUEST_TYPE_KEY).getS();
            mitigationTemplate = item.get(MITIGATION_TEMPLATE_NAME_KEY).getS();
            workflowId = Long.parseLong(item.get(WORKFLOW_ID_KEY).getN());
            workflowStatus = item.get(WORKFLOW_STATUS_KEY).getS();
        }
    }
}

package com.amazon.lookout.mitigation.service.activity.helper;

import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import lombok.NonNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.amazon.aws158.commons.dynamo.RouterMetadataConstants;
import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.mitigation.router.model.RouterFilterInfoWithMetadata;
import com.amazon.lookout.mitigation.service.CompositeAndConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationInstanceStatus;
import com.amazon.lookout.mitigation.service.MitigationRequestDescription;
import com.amazon.lookout.mitigation.service.MitigationRequestDescriptionWithStatuses;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.ListActiveMitigationsForServiceActivity;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationStatus;
import com.amazon.lookout.mitigation.service.mitigation.model.WorkflowStatus;
import com.amazon.lookout.mitigation.service.router.helper.RouterFilterInfoDeserializer;
import com.amazon.lookout.mitigation.utilities.POPLocationToRouterNameHelper;
import com.amazon.lookout.model.RequestType;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.mallardsoft.tuple.Pair;

/**
 * Helper to fetch the router metadata that is stored in the RouterMetadata DDB tables. Implements Callable to be allowed to run as a task in a separate thread.
 */
public class DDBBasedRouterMetadataHelper implements Callable<List<MitigationRequestDescriptionWithStatuses>> {
    private static final Log LOG = LogFactory.getLog(DDBBasedRouterMetadataHelper.class);

    private static final int MAX_DDB_OPERATION_ATTEMPTS = 5;
    private static final int SLEEP_MULTIPLER_BETWEEN_RETRIES_IN_MILLIS = 200;
    
    public static final String ROUTER_MITIGATION_UI = "RouterMitigationUI";
    public static final String ROUTER_MITIGATION_DEFAULT_TEMPLATE = "None";
    public static final int ROUTER_MITIGATION_DEFAULT_VERSION = 1;
    public static final int ROUTER_MITIGATION_DEFAULT_JOB_ID = 0;

    // Dates stored for the router mitigations carried out from the router mitigation UI are of the form: "Fri Sep 26 11:21:09 PDT 2014"
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("E MMMM dd HH:mm:ss z yyyy");
    
    private final ObjectMapper jsonObjectMapper = new ObjectMapper();
    
    private final String routerMetadataTableName;
    private final AmazonDynamoDBClient dynamoDBClient;
    private final ServiceSubnetsMatcher serviceSubnetsMatcher;
    private final POPLocationToRouterNameHelper locationToRouterNameHelper;
    
    @ConstructorProperties({"dynamoDBClient", "domain", "serviceSubnetsMatcher", "locationRouterMapper"})
    public DDBBasedRouterMetadataHelper(@NonNull AmazonDynamoDBClient dynamoDBClient, @NonNull String domain, 
    		                            @NonNull ServiceSubnetsMatcher serviceSubnetsMatcher, @NonNull POPLocationToRouterNameHelper locationToRouterNameHelper) {
        this.dynamoDBClient = dynamoDBClient;
        this.serviceSubnetsMatcher = serviceSubnetsMatcher;
        this.routerMetadataTableName = RouterMetadataConstants.DYNAMO_DB_TABLE_PREFIX + domain.toUpperCase();
        this.locationToRouterNameHelper = locationToRouterNameHelper;
    }
    
    /**
     * Returns a List of MitigationRequestDescriptionWithStatuses instances, each corresponding to a router mitigation stored in the router metadata table.
     * Note: the return list is a non-consolidated list of instances, in that, if 2 locations have the same mitigation description, they would show up as 2 different entries in this list.
     */
    public List<MitigationRequestDescriptionWithStatuses> call() {
        // List of mitigation descriptions to be returned as a response.
        List<MitigationRequestDescriptionWithStatuses> mitigationDescriptions = new ArrayList<>();
        
        // We need to perform a scan of the request table to fetch all the router mitigations.
        ScanRequest request = new ScanRequest(routerMetadataTableName);
        
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            request.setExclusiveStartKey(lastEvaluatedKey);
            ScanResult scanResult = null;
            
            Exception lastCaughtException = null;
            int attemptNum = 0;
            while (attemptNum++ < MAX_DDB_OPERATION_ATTEMPTS) {
                try {
                    scanResult = dynamoDBClient.scan(request);
                    break;
                } catch (Exception ex) {
                    lastCaughtException = ex;
                    LOG.warn("Caught exception when scanning table: " + routerMetadataTableName, ex);
                    
                    if (attemptNum < MAX_DDB_OPERATION_ATTEMPTS) {
                        try {
                            Thread.sleep(SLEEP_MULTIPLER_BETWEEN_RETRIES_IN_MILLIS * attemptNum);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }
            
            if (attemptNum > MAX_DDB_OPERATION_ATTEMPTS) {
                // AttemptNum is 1 more than the totalAttempts since the attemptNum is incremented in the while loop check above.
                int totalAttempts = attemptNum - 1;
                String msg = "Unable to scan results from table: " + routerMetadataTableName + " after: " + totalAttempts;
                LOG.error(msg, lastCaughtException);
                throw new RuntimeException(msg, lastCaughtException);
            }
            
            lastEvaluatedKey = scanResult.getLastEvaluatedKey();
            
            List<Map<String, AttributeValue>> items = scanResult.getItems();
            
            // For each item, extract the pair of filterInfoAsJSON and routerName, which are then used to convert into MitigationRequestDescriptionWithStatuses instances.
            List<Pair<String, String>> filterInfoAndRouterNameList = new ArrayList<>();
            
            for (Map<String, AttributeValue> item : items) {
                String routerName = item.get(RouterMetadataConstants.ROUTER_KEY).getS();
                
                for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                    if (entry.getKey().endsWith(RouterMetadataConstants.FILTER_JSON_DESCRIPTION)) {
                        String filterInfoAsJSON = entry.getValue().getS();
                        filterInfoAndRouterNameList.add(Pair.from(filterInfoAsJSON, routerName));
                    }
                }
            }
            
            // For each filterInfoAsJSON, convert it into the MitigationRequestDescriptionWithStatuses instance and add it to the list of mitigationDescriptions
            for (Pair<String, String> filterInfoAndRouterName : filterInfoAndRouterNameList) {
                String filterInfoAsJSON = Pair.get1(filterInfoAndRouterName);
                String routerName = Pair.get2(filterInfoAndRouterName);
                MitigationRequestDescriptionWithStatuses requestDescriptionWithStatuses = convertToMitigationRequestDescription(filterInfoAsJSON, routerName);
                
                // If this mitigation was created by the mitigation service tool, then skip adding this mitigation from the router mitigation metadata.
                if (requestDescriptionWithStatuses.getMitigationRequestDescription().getJobId() > 0) {
                    continue;
                }
                
                mitigationDescriptions.add(requestDescriptionWithStatuses);
            }
        } while (lastEvaluatedKey != null);
        
        return mitigationDescriptions;
    }
    
    /**
     * Helper method to convert a RouterFilterInfoWithMetadata represented as JSON string into an instance of MitigationRequestDescriptionWithStatuses. Protected for unit-testing.
     * @param filterInfoAsJSON RouterFilterInfoWithMetadata represented as JSON string.
     * @param routerName RouterName on which this mitigation is applied.
     * @return MitigationRequestDescriptionWithStatuses instance representing the RouterFilterInfoWithMetadata whose JSON represntation is passed as input.
     */
    protected MitigationRequestDescriptionWithStatuses convertToMitigationRequestDescription(@NonNull String filterInfoAsJSON, @NonNull String routerName) {
        try {
            // Use ObjectMapper to map the JSON string into a RouterFilterInfoWithMetadata instance.
            RouterFilterInfoWithMetadata filterInfo = jsonObjectMapper.readValue(filterInfoAsJSON, RouterFilterInfoWithMetadata.class);
            
            // Using information from the filterInfo instance, convert it into a MitigationRequestDescription instance.
            MitigationRequestDescription description = new MitigationRequestDescription();
            description.setDeviceName(DeviceName.POP_ROUTER.name());
            description.setDeviceScope(DeviceScope.GLOBAL.name());
            description.setJobId(filterInfo.getJobId());
            description.setMitigationActionMetadata(RouterFilterInfoDeserializer.convertToActionMetadata(filterInfo));
            description.setMitigationDefinition(RouterFilterInfoDeserializer.convertToMitigationDefinition(filterInfo));
            description.setMitigationName(filterInfo.getFilterName());
            description.setRequestDate(formatter.parseDateTime(filterInfo.getLastDatePushedToRouter()).getMillis());
            
            // Now create an instance of MitigationInstanceStatus, defaulting the mitigation status to DEPLOY_SUCCEEDED.
            MitigationInstanceStatus instanceStatus = new MitigationInstanceStatus();
            String location = locationToRouterNameHelper.getLocationFromRouterName(routerName);
            instanceStatus.setLocation(location);
            
            if (filterInfo.isEnabled()) {
                instanceStatus.setMitigationStatus(MitigationStatus.DEPLOY_SUCCEEDED);
            } else {
                instanceStatus.setMitigationStatus(MitigationStatus.DISABLED);
            }
            
            Map<String, MitigationInstanceStatus> instancesStatus = new HashMap<>();
            instancesStatus.put(location, instanceStatus);
            
            // Using the MitigationRequestDescription and MitigationInstanceStatus above, build a new instance of MitigationRequestDescriptionWithStatuses.
            MitigationRequestDescriptionWithStatuses descriptionWithStatuses = new MitigationRequestDescriptionWithStatuses();
            descriptionWithStatuses.setMitigationRequestDescription(description);
            descriptionWithStatuses.setInstancesStatusMap(instancesStatus);
            return descriptionWithStatuses;
        } catch (Exception ex) {
            String msg = "Unable to deserialize filterInfo: " + filterInfoAsJSON + " into an instance of MitigationRequestDescriptionWithStatuses";
            LOG.error(msg, ex);
            throw new RuntimeException(msg, ex);
        }
    }
    
    /**
     * Helper method to merge mitigations fetched from the router metadata DDB table, with the mitigations fetched from the LookoutMitigationService.
     * 
     * Merging algorithm:
     * Iterate through the list of MitigationRequestDescriptionWithStatuses instances corresponding to the router mitigations:
     * 1. Check if the same mitigationName already exists in the currentlyActiveMitigations.
     * 
     * 2.1 If it does, then perform a deeper check to check if the mitigation from routerMetadata and the one from the mitigationService have exactly the same definition.
     * 2.2 If it does, then ensure there is an entry for this location in the MitigationInstancesStatus map.
     * 
     * 3.1 If the checks in #1 and #2.1 aren't satisfied, then check which customer this mitigation belongs to, based on the destIPs if present.
     * 3.2 If we cannot figure out any service to which this mitigation belongs, then we default to adding this mitigation in the return values.
     * 3.3 If we figure out the services to which this mitigation belongs and if none of those services match the input serviceName, then skip this mitigation from the list of return values.
     * 3.4 Else, if the services to which this mitigation belongs contains the service passed as input, then consider this mitigation in the list of return values.
     * 
     * @param currentlyActiveMitigations Map of String (key formed using deviceName and mitigationName) to a list of MitigationRequestDescriptionWithStatuses instances, identifying mitigations which must be returned back to the caller.
     *                                    Note, the List exists in the value only to support the case where for the same key (deviceName and mitigationName), we might have an entry for an active mitigation
     *                                    and also an ongoing edit request for that same mitigation.
     * @param routerMitigations List of MitigationRequestDescriptionWithStatuses instances which were fetched from the router metadata DDB table.
     * @param serviceName Service for which these mitigations are being fetched/merged.
     * @return List of MitigationRequestDescriptionWithStatuses instances, after merging the mitigations fetched from the router metadata table with the ones fetched from the mitigation service tables.
     */
    public List<MitigationRequestDescriptionWithStatuses> mergeMitigations(@NonNull Map<String, List<MitigationRequestDescriptionWithStatuses>> currentlyActiveMitigations, 
                                                                           @NonNull List<MitigationRequestDescriptionWithStatuses> routerMitigations, @NonNull String serviceName) {
        for (MitigationRequestDescriptionWithStatuses routerMitigation : routerMitigations) {
            MitigationRequestDescription routerMitigationDescription = routerMitigation.getMitigationRequestDescription();
            String routerMitigationKey = ListActiveMitigationsForServiceActivity.createDeviceAndMitigationNameKey(routerMitigation);
            
            // Flag to indicate if we found a matching mitigation from the mitigations driven through the mitigation service.
            boolean matchingMitigationFound = false;
            
            // Flag to indicate if this current router mitigation needs to be added to the final response of MitigationRequestDescriptionWithStatuses instances.
            boolean addRouterMitigationToReturnValues = false;
            
            // Check if the request from the router mitigation metadata store has the same key in the currentlyActiveMitigations Map.
            if (currentlyActiveMitigations.containsKey(routerMitigationKey)) {
                // Perform a deeper check to see if the mitigation definitions match any of the currentlyActiveMitigations descriptions.
                for (MitigationRequestDescriptionWithStatuses mitServiceMitigation : currentlyActiveMitigations.get(routerMitigationKey)) {
                    MitigationDefinition routerMitigationDefinition = routerMitigationDescription.getMitigationDefinition();
                    MitigationDefinition mitServiceMitigationDefinition = mitServiceMitigation.getMitigationRequestDescription().getMitigationDefinition();
                    
                    if (routerMitigationDefinition.equals(mitServiceMitigationDefinition)) {
                        // Update the instanceStatus map in the mergedMitigation to ensure this location is included in list of locations for this mitigation.
                        mitServiceMitigation.getInstancesStatusMap().putAll(routerMitigation.getInstancesStatusMap());
                        
                        matchingMitigationFound = true;
                        break;
                    }
                }
            }
            
            // If the mitigation from routerMetadata matches a currentlyActiveMitigations mitigation, then as per Step 2.1 defined above, the locations must have already been updated, so do nothing more.
            if (matchingMitigationFound) {
                continue;
            }
            
            // This router mitigation wasn't found in the currentlyActiveMitigations. So now we execute the steps 3.1 - 3.4 listed above.
            
            List<String> destSubnets = getDestSubnetsFromRouterMitigationConstraint(routerMitigationDescription.getMitigationDefinition().getConstraint());
            
            if (destSubnets.isEmpty()) {
                // If there are no destIPs in the constraint, we don't know which service this mitigation belongs to, then simply default to adding this mitigation for the current serviceName.
                addRouterMitigationToReturnValues = true;
            } else {
                // If we don't know which service this mitigation belongs to or we know that at least 1 destIP belongs to the serviceName passed as input, 
                // then mark this mitigation to be added for the current serviceName.
                Set<String> servicesForDestSubnets = serviceSubnetsMatcher.getAllServicesForSubnets(destSubnets);
                if (servicesForDestSubnets.isEmpty() || servicesForDestSubnets.contains(serviceName)) {
                    addRouterMitigationToReturnValues = true;
                }
            }
            
            // If we have deemed this mitigation to be added to the current set of return values, then update some of the required fields for the 
            // MitigationRequestDescriptionWithStatuses instance to default values, before adding it into the list of mitigations to be returned.
            if (addRouterMitigationToReturnValues) {
                routerMitigationDescription.setServiceName(serviceName);
                routerMitigationDescription.setMitigationTemplate(ROUTER_MITIGATION_DEFAULT_TEMPLATE);
                routerMitigationDescription.setMitigationVersion(ROUTER_MITIGATION_DEFAULT_VERSION);
                routerMitigationDescription.setUpdateJobId(ROUTER_MITIGATION_DEFAULT_JOB_ID);
                routerMitigationDescription.setNumPreDeployChecks(0);
                routerMitigationDescription.setNumPostDeployChecks(0);
                routerMitigationDescription.setRequestStatus(WorkflowStatus.SUCCEEDED);
                routerMitigationDescription.setRequestType(RequestType.CreateRequest.name());
                
                if (currentlyActiveMitigations.containsKey(routerMitigationKey)) {
                    currentlyActiveMitigations.get(routerMitigationKey).add(routerMitigation);
                } else {
                    currentlyActiveMitigations.put(ListActiveMitigationsForServiceActivity.createDeviceAndMitigationNameKey(routerMitigation), Lists.newArrayList(routerMitigation));
                }
            }
        }
        
        // List of MitigationRequestDescriptionWithStatuses instances to be returned as a result of merging the mitigations.
        List<MitigationRequestDescriptionWithStatuses> mergedMitigations = new ArrayList<>();
        for (List<MitigationRequestDescriptionWithStatuses> mitigationsFromMitService : currentlyActiveMitigations.values()) {
            mergedMitigations.addAll(mitigationsFromMitService);
        }
        return mergedMitigations;
    }
    
    /**
     * Extracts dest subnets from a given Constraint instance. Protected for unit-testing.
     * @param constraint Constraint instance from which to extract out any dest subnets if present.
     * @return List of String representing the dest subnets present in the Constraint, empty if there isn't any dest based constraint.
     */
    protected List<String> getDestSubnetsFromRouterMitigationConstraint(Constraint constraint) {
        List<String> destSubnets = new ArrayList<>();
        
        if (constraint instanceof SimpleConstraint) {
            String attributeName = ((SimpleConstraint) constraint).getAttributeName();
            if (attributeName.equals(PacketAttributesEnumMapping.DESTINATION_IP.name())) {
                destSubnets.addAll(((SimpleConstraint) constraint).getAttributeValues());
            }
        } else {
            if (constraint instanceof CompositeAndConstraint) {
                List<Constraint> subConstraints = ((CompositeAndConstraint) constraint).getConstraints();
                for (Constraint subConstraint : subConstraints) {
                    destSubnets.addAll(getDestSubnetsFromRouterMitigationConstraint(subConstraint));
                }
            }
        }
        
        return destSubnets;
    }
}

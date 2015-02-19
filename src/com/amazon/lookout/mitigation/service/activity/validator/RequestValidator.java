package com.amazon.lookout.mitigation.service.activity.validator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.CollectionUtils;

import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.GetMitigationInfoRequest;
import com.amazon.lookout.mitigation.service.GetRequestStatusRequest;
import com.amazon.lookout.mitigation.service.ListActiveMitigationsForServiceRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationModificationRequest;
import com.amazon.lookout.mitigation.service.ReportInactiveLocationRequest;
import com.amazon.lookout.mitigation.service.activity.helper.ServiceLocationsHelper;
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * RequestValidator is a basic validator for requests to ensure the requests are well-formed and contain all the required inputs.
 * This validator performs template-agnostic validation - it doesn't dive deep into specific requirements of each template, the TemplateBasedRequestValidator
 * is responsible for such checks.  
 *
 */
@AllArgsConstructor
@ThreadSafe
public class RequestValidator {
    private static final Log LOG = LogFactory.getLog(RequestValidator.class);
    
    // Maintain a Set of the mitigationTemplate, making it easier to search if a single template is part of the set of defined templates.
    private final Set<String> mitigationTemplates = Sets.newHashSet(MitigationTemplate.values());
    
    // Maintain a Set of the supported serviceNames, making it easier to search if a single serviceName is part of the set of defined serviceNames.
    private final Set<String> serviceNames = Sets.newHashSet(ServiceName.values());
    
    private static final int DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS = 100;
    private static final int MAX_LENGTH_MITIGATION_DESCRIPTION = 500;
    
    private static final int MAX_NUMBER_OF_LOCATIONS = 200;
    private static final int MAX_NUMBER_OF_TICKETS = 10;
    
    @NonNull private final ServiceLocationsHelper serviceLocationsHelper;
    
    /**
     * Validates if the request object passed to the CreateMitigationAPI is valid.
     * @param request MitigationModificationRequest representing the input to the CreateMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateCreateRequest(@NonNull CreateMitigationRequest request) {
        validateCommonModificationRequestParameters(request);
        
        if (request.getMitigationDefinition() == null) {
            String msg = "No mitigation definition found.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We only check for the constraint and not the action, since for some templates, it is valid for requests to not have any actions.
        // Template-based validations should take care of specific checks related to their template.
        if (request.getMitigationDefinition().getConstraint() == null) {
            String msg = "No constraint found for mitigation definition.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the DeleteMitigationAPI is valid.
     * @param request Instance of DeleteMitigationFromAllLocationsRequest representing the input to the DeleteMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateDeleteRequest(@NonNull DeleteMitigationFromAllLocationsRequest request) {
        validateCommonModificationRequestParameters(request);
        
        if (request.getMitigationVersion() < 1) {
            String msg = "Version of the mitigation to be deleted should be set to >=1, instead found: " + request.getMitigationVersion();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the GetRequestStatus API is valid
     * @param A GetRequestStatusRequest object representing the input to the GetRequestStatus API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetRequestStatusRequest(@NonNull GetRequestStatusRequest request) {
        if (request.getJobId() < 1) {
            String msg = "Job IDs should be >= 1, instead found: " + request.getJobId();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateDeviceName(request.getDeviceName());
        validateServiceName(request.getServiceName());
        validateMitigationTemplate(request.getMitigationTemplate());
    }
    
    /**
     * Validates if the request object passed to the GetMitigationInfo API is valid
     * @param An instance of GetMitigationInfoRequest representing the input to the GetMitigationInfo API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetMitigationInfoRequest(@NonNull GetMitigationInfoRequest request) {
        validateServiceName(request.getServiceName());
        validateDeviceName(request.getDeviceName());
        validateDeviceScope(request.getDeviceScope());
        validateMitigationName(request.getMitigationName());
    }
    
    /**
     * Validates if the request object passed to the ListActiveMitigationsForService API is valid
     * @param A ListActiveMitigationsForServiceRequest object representing the input to the ListActiveMitigationsForService API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateListActiveMitigationsForServiceRequest(@NonNull ListActiveMitigationsForServiceRequest request) {
        String serviceName = request.getServiceName();
        validateServiceName(serviceName);
        validateListOfLocations(request.getLocations(), serviceName);
        validateDeviceName(request.getDeviceName());
    }
    
    /**
     * Validates if the request object passed to the ReportInactiveLocation API is valid
     * @param A ReportInactiveLocationRequest object representing the input to the ReportInactiveLocation API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateReportInactiveLocation(@NonNull ReportInactiveLocationRequest request) {
        String serviceName = request.getServiceName();
        validateServiceName(serviceName);
        validateDeviceName(request.getDeviceName());
        validateListOfLocations(Lists.newArrayList(request.getLocation()), serviceName);
    }
    
    /**
     * Private helper method to validate the common parameters for some of the modification requests.
     * @param mitigationName Name of the mitigation passed in the request by the client.
     * @param mitigationTemplate Template corresponding to the mitigation in the request by the client.
     * @param serviceName Service corresponding to the mitigation in the request by the client.
     * @param actionMetadata Instance of MitigationActionMetadata specifying some of the metadata related to this action.
     */
    private void validateCommonModificationRequestParameters(@NonNull MitigationModificationRequest request) {
        validateMitigationName(request.getMitigationName());
        
        validateMitigationTemplate(request.getMitigationTemplate());
        
        validateServiceName(request.getServiceName());
        
        MitigationActionMetadata actionMetadata = request.getMitigationActionMetadata();
        if (actionMetadata == null) {
            String msg = "No MitigationActionMetadata found! Passing the MitigationActionMetadata is mandatory for performing any actions using the MitigationService.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        validateUserName(actionMetadata.getUser());
        validateToolName(actionMetadata.getToolName());
        validateMitigationDescription(actionMetadata.getDescription());
        
        if (actionMetadata.getRelatedTickets() != null) {
            validateRelatedTickets(actionMetadata.getRelatedTickets());
        }
    }
    
    private void validateDeviceName(String deviceName) {
        if (isInvalidFreeFormText(deviceName, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid device name found! Valid device names: " + DeviceName.values();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceName is not defined within the DeviceName enum.
        try {
            DeviceName.valueOf(deviceName);
        } catch (Exception ex) {
            String msg = "The device name that was provided, " + deviceName + ", is not a valid name. Valid deviceNames: " + DeviceName.values();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateServiceName(String serviceName) {
        if (isInvalidFreeFormText(serviceName, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid service name found: " + serviceName + ". Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!serviceNames.contains(serviceName)) {
            String msg = "Invalid service name found: " + serviceName + ". Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateDeviceScope(String deviceScope) {
        if (isInvalidFreeFormText(deviceScope, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid device scope found! Valid device scopes: " + DeviceScope.values();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceScope is not defined within the DeviceScope enum.
        try {
            DeviceScope.valueOf(deviceScope);
        } catch (Exception ex) {
            String msg = "The device scope that was provided, " + deviceScope + ", is not valid. Valid device scopes: " + DeviceScope.values();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateMitigationName(String mitigationName) {
        if (isInvalidFreeFormText(mitigationName, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid mitigation name found! A valid mitigation name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateMitigationTemplate(String mitigationTemplate) {
        if (isInvalidFreeFormText(mitigationTemplate, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid mitigation template found! Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!mitigationTemplates.contains(mitigationTemplate)) {
            String msg = "Invalid mitigation template found: " + mitigationTemplate + ". Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateUserName(String userName) {
        if (isInvalidFreeFormText(userName, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid user name found! A valid user name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateToolName(String toolName) {
        if (isInvalidFreeFormText(toolName, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
            String msg = "Invalid tool name found! A valid tool name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateMitigationDescription(String mitigationDescription) {
        if (isInvalidFreeFormText(mitigationDescription, MAX_LENGTH_MITIGATION_DESCRIPTION)) {
            String msg = "Invalid description found! A valid description must contain more than 0 and less than: " + MAX_LENGTH_MITIGATION_DESCRIPTION + " ascii-printable characters.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    private void validateRelatedTickets(List<String> relatedTickets) {
        if (relatedTickets.size() > MAX_NUMBER_OF_TICKETS) {
            String msg = "Exceeded the number of tickets that can be specified for a single mitigation. Max allowed: " + MAX_NUMBER_OF_TICKETS +
                         ". Instead found: " + relatedTickets.size() + " number of tickets.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        Set<String> setOfRelatedTickets = new HashSet<String>(relatedTickets);
        if (setOfRelatedTickets.size() != relatedTickets.size()) {
            String msg = "Duplicate related tickets found in actionMetadata: " + relatedTickets;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // We currently (02/2015) don't constraint the user of how they want to input the tickets, either as just ids (0043589677) or 
        // abbreviated link: tt/0043589677 or the entire link: https://tt.amazon.com/0043589677. Hence simply checking to just ensure we have all valid characters for each of those.
        for (String ticket : relatedTickets) {
            if (isInvalidFreeFormText(ticket, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
                String msg = "Invalid ticket reference found! A valid ticket must contain either just the ticket number (eg: 0043589677) or links " +
                             "to the ticket (eg: tt/0043589677 or https://tt.amazon.com/0043589677)";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            if (!TICKETS_PATTERN.matcher(ticket).matches()) {
                String msg = "Invalid ticket reference found! Found value: " + ticket + ", but a valid ticket must contain either just the ticket number " +
                             "(eg: 0043589677) or links to the ticket of the form: tt/0043589677 or https://tt.amazon.com/0043589677";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    private void validateListOfLocations(List<String> locations, String serviceName) {
        if (locations != null) {
            if (locations.size() > MAX_NUMBER_OF_LOCATIONS) {
                String msg = "Exceeded the number of locations that can be specified for a single request. Max allowed: " + MAX_NUMBER_OF_LOCATIONS +
                             ". Instead found: " + locations.size() + " number of locations.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            if (CollectionUtils.isEmpty(locations)) {
                String msg = "Empty list of locations found! Either the locations should be null or must contain valid values.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
            
            Set<String> validLocationsForService = serviceLocationsHelper.getLocationsForService(serviceName).orNull();
            List<String> invalidLocationsInRequest = new ArrayList<>();
            for (String location : locations) {
                if (isInvalidFreeFormText(location, DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS)) {
                    String msg = "Invalid location name found! A valid location name must contain more than 0 and less than: " + DEFAULT_MAX_LENGTH_USER_INPUT_STRINGS + " ascii-printable characters.";
                    LOG.info(msg);
                    throw new IllegalArgumentException(msg);
                }
                
                if ((validLocationsForService != null) && !validLocationsForService.contains(location)) {
                    invalidLocationsInRequest.add(location);
                }
            }
            
            if (!invalidLocationsInRequest.isEmpty()) {
                String msg = "Invalid location name found! Locations: " + invalidLocationsInRequest + " aren't valid for service: " + serviceName;
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
    protected boolean isInvalidFreeFormText(String stringToCheck, int maxLength) {
        boolean isBlank = StringUtils.isBlank(stringToCheck);
        boolean containsOnlyPrintableCharacters = StringUtils.isAsciiPrintable(stringToCheck);
        boolean exceedsDefaultLength = ((stringToCheck != null) ? (stringToCheck.length() > maxLength) : false);
        return (isBlank || !containsOnlyPrintableCharacters || exceedsDefaultLength);
    }
}

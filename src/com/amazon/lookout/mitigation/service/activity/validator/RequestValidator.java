package com.amazon.lookout.mitigation.service.activity.validator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
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
import com.amazon.lookout.mitigation.service.constants.DeviceName;
import com.amazon.lookout.mitigation.service.constants.DeviceScope;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.google.common.collect.Sets;

/**
 * RequestValidator is a basic validator for requests to ensure the requests are well-formed and contain all the required inputs.
 * This validator performs template-agnostic validation - it doesn't dive deep into specific requirements of each template, the TemplateBasedRequestValidator
 * is responsible for such checks.  
 *
 */
@ThreadSafe
public class RequestValidator {
    private static final Log LOG = LogFactory.getLog(RequestValidator.class);
    
    // Maintain a Set of the mitigationTemplate, making it easier to search if a single template is part of the set of defined templates.
    private final Set<String> mitigationTemplates = Sets.newHashSet(MitigationTemplate.values());
    
    // Maintain a Set of the supported serviceNames, making it easier to search if a single serviceName is part of the set of defined serviceNames.
    private final Set<String> serviceNames = Sets.newHashSet(ServiceName.values());
    
    /**
     * Validates if the request object passed to the CreateMitigationAPI is valid.
     * @param request MitigationModificationRequest representing the input to the CreateMitigationAPI.
     * @return void. Doesn't return any values. Will throw back an IllegalArgumentException if any of the input parameters aren't considered valid.
     */
    public void validateCreateRequest(@Nonnull CreateMitigationRequest request) {
        Validate.notNull(request);
        validateCommonRequestParameters(request);
        
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
    public void validateDeleteRequest(@Nonnull DeleteMitigationFromAllLocationsRequest request) {
        Validate.notNull(request);
        validateCommonRequestParameters(request);
        
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
    public void validateGetRequestStatusRequest(@Nonnull GetRequestStatusRequest request) {
        Validate.notNull(request);
        
        if (request.getJobId() < 1) {
            String msg = "Job IDs should be >= 1, instead found: " + request.getJobId();
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        String deviceName = request.getDeviceName();
        if (StringUtils.isEmpty(deviceName)) {
            String msg = "Null or empty device name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceName is not defined within the DeviceName enum.
        try {
            DeviceName.valueOf(deviceName);
        } catch (Exception ex) {
            String msg = "The device name that was provided, " + deviceName + ", is not a valid name.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        String serviceName = request.getServiceName();
        if (StringUtils.isEmpty(serviceName)) {
            String msg = "Null or empty service name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the serviceName is not defined within the ServiceName enum.
        if(!serviceNames.contains(serviceName)) {
            String msg = "The service name that was provided, " + serviceName + ", is not a valid name";
            throw new IllegalArgumentException(msg);
        }
        
        String mitigationTemplate = request.getMitigationTemplate();
        if (StringUtils.isEmpty(mitigationTemplate)) {
            String msg = "Null or empty mitigationTemplate found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the mitigationTemplate is not defined within the MitigationTemplate enum.
        if(!Arrays.asList(MitigationTemplate.values()).contains(mitigationTemplate)) {
            String msg = "The mitigationTemplate that was provided, " + mitigationTemplate + ", is not a valid mitigationTemplate";
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the GetMitigationInfo API is valid
     * @param An instance of GetMitigationInfoRequest representing the input to the GetMitigationInfo API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateGetMitigationInfoRequest(@Nonnull GetMitigationInfoRequest request) {
        Validate.notNull(request);
        
        // Validate service name.
        String serviceName = request.getServiceName();
        if (StringUtils.isEmpty(serviceName)) {
            String msg = "Null or empty service name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the serviceName is not defined within the ServiceName enum.
        if(!serviceNames.contains(serviceName)) {
            String msg = "The service name that was provided, " + serviceName + ", is not a valid name";
            throw new IllegalArgumentException(msg);
        }
        
        // Validate device name.
        String deviceName = request.getDeviceName();
        if (StringUtils.isEmpty(deviceName)) {
            String msg = "Null or empty device name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceName is not defined within the DeviceName enum.
        try {
            DeviceName.valueOf(deviceName);
        } catch (Exception ex) {
            String msg = "The device name that was provided, " + deviceName + ", is not a valid name.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // Validate device scope.
        String deviceScope = request.getDeviceScope();
        if (StringUtils.isEmpty(deviceScope)) {
            String msg = "Null or empty device scope found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the deviceScope is not defined within the DeviceScope enum.
        try {
            DeviceScope.valueOf(deviceScope);
        } catch (Exception ex) {
            String msg = "The device scope that was provided, " + deviceScope + ", is not valid.";
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        String mitigationName = request.getMitigationName();
        if (StringUtils.isEmpty(mitigationName)) {
            String msg = "Null or empty mitigationName found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Validates if the request object passed to the ListActiveMitigationsForService API is valid
     * @param A ListActiveMitigationsForServiceRequest object representing the input to the ListActiveMitigationsForService API
     * @return void No values are returned but it will throw back an IllegalArgumentException if any of the parameters aren't considered valid.
     */
    public void validateListActiveMitigationsForServiceRequest(@Nonnull ListActiveMitigationsForServiceRequest request) {
        Validate.notNull(request);
        
        List<String> locations = request.getLocations();
        if (locations != null && CollectionUtils.isEmpty(request.getLocations())) {
            String msg = "Empty list of locations found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (locations != null && locations.contains("")) {
            String msg = "Empty String found in the List of locations in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        String deviceName = request.getDeviceName();
        if (StringUtils.isBlank(deviceName)) {
            String msg = "Null or empty device name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (deviceName != null) {       
            // This will throw an exception if the deviceName is not defined within the DeviceName enum.
            try {
                DeviceName.valueOf(deviceName);  
            } catch (Exception ex) {
                String msg = "The device name that was provided, " + deviceName + ", is not a valid name.";
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
        
        String serviceName = request.getServiceName();
        if (StringUtils.isBlank(serviceName)) {
            String msg = "Null or empty service name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        // This will throw an exception if the serviceName is not defined within the ServiceName enum.
        if(!Arrays.asList(ServiceName.values()).contains(serviceName)) {
            String msg = "The service name that was provided, " + serviceName + ", is not a valid name.";
            throw new IllegalArgumentException(msg);
        }
    }
    
    /**
     * Private helper method to validate the common parameters for some of the modification requests.
     * @param mitigationName Name of the mitigation passed in the request by the client.
     * @param mitigationTemplate Template corresponding to the mitigation in the request by the client.
     * @param serviceName Service corresponding to the mitigation in the request by the client.
     * @param actionMetadata Instance of MitigationActionMetadata specifying some of the metadata related to this action.
     */
    private void validateCommonRequestParameters(MitigationModificationRequest request) {
        String mitigationName = request.getMitigationName();
        if (StringUtils.isEmpty(mitigationName)) {
            String msg = "Null or empty mitigation name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        String mitigationTemplate = request.getMitigationTemplate();
        if (StringUtils.isEmpty(mitigationTemplate)) {
            String msg = "Null or empty mitigation template found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!mitigationTemplates.contains(mitigationTemplate)) {
            String msg = "Invalid mitigation template found in request: " + ReflectionToStringBuilder.toString(request) +
                         ". Valid mitigation templates are: " + mitigationTemplates;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        String serviceName = request.getServiceName();
        if (StringUtils.isEmpty(serviceName)) {
            String msg = "Null or empty service name found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (!serviceNames.contains(serviceName)) {
            String msg = "Invalid service name found in request: " + ReflectionToStringBuilder.toString(request) +
                         " Valid service names are: " + serviceNames;
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        MitigationActionMetadata actionMetadata = request.getMitigationActionMetadata();
        if (actionMetadata == null) {
            String msg = "No MitigationActionMetadata found in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if ((actionMetadata.getUser() == null) || (actionMetadata.getUser().isEmpty())) {
            String msg = "No user defined in the mitigation action metadata in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(actionMetadata.getToolName())) {
            String msg = "No tool specified in the mitigation action metadata in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (StringUtils.isEmpty(actionMetadata.getDescription())) {
            String msg = "No description specified in the mitigation action metadata in request: " + ReflectionToStringBuilder.toString(request);
            LOG.info(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (actionMetadata.getRelatedTickets() != null) {
            Set<String> setOfRelatedTickets = new HashSet<String>(actionMetadata.getRelatedTickets());
            if (setOfRelatedTickets.size() != actionMetadata.getRelatedTickets().size()) {
                String msg = "Duplicate related tickets found in actionMetadata in the request: " + ReflectionToStringBuilder.toString(request);
                LOG.info(msg);
                throw new IllegalArgumentException(msg);
            }
        }
    }
    
}

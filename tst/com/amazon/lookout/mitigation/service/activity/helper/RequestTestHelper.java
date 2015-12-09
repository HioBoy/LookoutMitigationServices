package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.mitigation.service.BlastRadiusCheck;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.amazon.lookout.mitigation.service.mitigation.model.ServiceName;
import com.google.common.collect.Lists;

public class RequestTestHelper {

    public static CreateMitigationRequest generateCreateMitigationRequest(String template, String name, String service) {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(name);
        request.setMitigationTemplate(template);
        request.setServiceName(service);
        
        MitigationActionMetadata metadata = generateActionMetadata("why not create?");
        request.setMitigationActionMetadata(metadata);
        
        MitigationDefinition definition = defaultMitigationDefinition();
        request.setMitigationDefinition(definition);
        
        BlastRadiusCheck check1 = new BlastRadiusCheck();
        DateTime now = new DateTime();
        check1.setEndDateTime(now.toString());
        check1.setStartDateTime(now.minusHours(1).toString());
        check1.setFailureThreshold(5.0);
        
        BlastRadiusCheck check2 = new BlastRadiusCheck();
        check2.setEndDateTime(now.toString());
        check2.setStartDateTime(now.minusHours(2).toString());
        check2.setFailureThreshold(10.0);
        
        List<MitigationDeploymentCheck> checks = new ArrayList<>();
        checks.add(check1);
        checks.add(check2);
        request.setPreDeploymentChecks(checks);
        
        return request;
    }

    private static MitigationActionMetadata generateActionMetadata(String description) {
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        metadata.setUser("lookout");
        metadata.setToolName("lookoutui");
        metadata.setDescription(description);
        metadata.setRelatedTickets(Lists.newArrayList("12345"));
        return metadata;
    }
    
    public static EditMitigationRequest generateEditMitigationRequest(String template, String name, String service, int version) {
        if (version <= 1) {
            throw new IllegalArgumentException("version must be > 1");
        }
        
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName(name);
        request.setMitigationTemplate(template);
        request.setServiceName(service);
        request.setMitigationVersion(version);
        
        MitigationActionMetadata metadata = generateActionMetadata("why not edit?");
        request.setMitigationActionMetadata(metadata);
        
        MitigationDefinition definition = 
                createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.5"));
        request.setMitigationDefinition(definition);
        
        BlastRadiusCheck check1 = new BlastRadiusCheck();
        DateTime now = new DateTime();
        check1.setEndDateTime(now.toString());
        check1.setStartDateTime(now.minusHours(1).toString());
        check1.setFailureThreshold(5.0);
        
        BlastRadiusCheck check2 = new BlastRadiusCheck();
        check2.setEndDateTime(now.toString());
        check2.setStartDateTime(now.minusHours(2).toString());
        check2.setFailureThreshold(10.0);
        
        List<MitigationDeploymentCheck> checks = new ArrayList<>();
        checks.add(check1);
        checks.add(check2);
        request.setPreDeploymentChecks(checks);
        
        return request;
    }
    
    public static DeleteMitigationFromAllLocationsRequest generateDeleteMitigationRequest(
            String template, String name, String service, int version) 
    {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName(name);
        request.setMitigationTemplate(template);
        request.setServiceName(service);
        request.setMitigationVersion(version);
        
        MitigationActionMetadata metadata = generateActionMetadata("why not delete?");
        request.setMitigationActionMetadata(metadata);
        
        return request;
    }

    public static CreateMitigationRequest generateCreateMitigationRequest(String template, String name) {
        return generateCreateMitigationRequest(template, name, ServiceName.Route53);
    }
    
    public static EditMitigationRequest generateEditMitigationRequest(String template, String name, int version) {
        return generateEditMitigationRequest(template, name, ServiceName.Route53, version);
    }
    
    public static DeleteMitigationFromAllLocationsRequest generateDeleteMitigationRequest(String template, String name, int version) {
        return generateDeleteMitigationRequest(template, name, ServiceName.Route53, version);
    }

    public static CreateMitigationRequest generateCreateMitigationRequest() {
        return generateCreateMitigationRequest("TestMitigationName");
    }
    
    public static CreateMitigationRequest generateCreateMitigationRequest(String name) {
        return generateCreateMitigationRequest(MitigationTemplate.Router_RateLimit_Route53Customer, name);
    }
    
    public static EditMitigationRequest generateEditMitigationRequest(String name, int version) {
        return generateEditMitigationRequest(MitigationTemplate.Router_RateLimit_Route53Customer, name, version);
    }
    
    public static DeleteMitigationFromAllLocationsRequest generateDeleteMitigationRequest(String name, int version) {
        return generateDeleteMitigationRequest(MitigationTemplate.Router_RateLimit_Route53Customer, name, version);
    }

    public static MitigationDefinition defaultMitigationDefinition() {
        return createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
    }

    public static MitigationDefinition createMitigationDefinition(String attrName, List<String> attrValues) {
        SimpleConstraint constraint = new SimpleConstraint();
        constraint.setAttributeName(attrName);
        constraint.setAttributeValues(attrValues);
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        return definition;
    }
}

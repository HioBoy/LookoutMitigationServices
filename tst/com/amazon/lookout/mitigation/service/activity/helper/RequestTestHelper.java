package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.joda.time.DateTime;

import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.mitigation.service.AlarmCheck;
import com.amazon.lookout.mitigation.service.Alarm;
import com.amazon.lookout.mitigation.service.CreateMitigationRequest;
import com.amazon.lookout.mitigation.service.DeleteMitigationFromAllLocationsRequest;
import com.amazon.lookout.mitigation.service.EditMitigationRequest;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.MitigationDeploymentCheck;
import com.amazon.lookout.mitigation.service.S3Object;
import com.amazon.lookout.mitigation.service.BlackWatchConfigBasedConstraint;
import com.amazon.lookout.mitigation.service.mitigation.model.MitigationTemplate;
import com.google.common.collect.Lists;

public class RequestTestHelper {

    public static CreateMitigationRequest generateCreateMitigationRequest(String template, String name) {
        CreateMitigationRequest request = new CreateMitigationRequest();
        request.setMitigationName(name);
        request.setMitigationTemplate(template);
        
        MitigationActionMetadata metadata = generateActionMetadata("why not create?");
        request.setMitigationActionMetadata(metadata);
        
        MitigationDefinition definition = defaultMitigationDefinition();
        request.setMitigationDefinition(definition);

        List<String> locations = new ArrayList<>();
        locations.add("POP1");

        List<MitigationDeploymentCheck> postChecks = new ArrayList<>();
        AlarmCheck alarmCheck = new AlarmCheck();
        alarmCheck.setCheckEveryNSec(5);
        alarmCheck.setCheckTotalPeriodSec(5);
        alarmCheck.setDelaySec(1);
        alarmCheck.setCheckTotalPeriodSec(6);
        Map<String, List<Alarm>> alarms = new HashMap<>();
        List<Alarm> alarmList = new ArrayList<>();
        Alarm covfefe = new Alarm();
        covfefe.setName("covfefe");
        alarmList.add(covfefe);
        alarms.put("covefefe", alarmList);
        alarmCheck.setAlarms(alarms);
        postChecks.add(alarmCheck);
        request.setPostDeploymentChecks(postChecks);

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
    
    public static EditMitigationRequest generateEditMitigationRequest(String template, String name, int version) {
        if (version <= 1) {
            throw new IllegalArgumentException("version must be > 1");
        }
        
        EditMitigationRequest request = new EditMitigationRequest();
        request.setMitigationName(name);
        request.setMitigationTemplate(template);
        request.setMitigationVersion(version);
        
        MitigationActionMetadata metadata = generateActionMetadata("why not edit?");
        request.setMitigationActionMetadata(metadata);
        
        MitigationDefinition definition = 
                createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.5"));
        request.setMitigationDefinition(definition);

        return request;
    }
    
    public static DeleteMitigationFromAllLocationsRequest generateDeleteMitigationRequest(
            String template, String name, int version) 
    {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
        
        DeleteMitigationFromAllLocationsRequest request = new DeleteMitigationFromAllLocationsRequest();
        request.setMitigationName(name);
        request.setMitigationTemplate(template);
        request.setMitigationVersion(version);
        
        MitigationActionMetadata metadata = generateActionMetadata("why not delete?");
        request.setMitigationActionMetadata(metadata);
        
        return request;
    }

    public static CreateMitigationRequest generateCreateMitigationRequest() {
        return generateCreateMitigationRequest("TestMitigationName");
    }
    
    public static CreateMitigationRequest generateCreateMitigationRequest(String name) {
        return generateCreateMitigationRequest(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, name);
    }
    
    public static EditMitigationRequest generateEditMitigationRequest(String name, int version) {
        return generateEditMitigationRequest(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, name, version);
    }
    
    public static DeleteMitigationFromAllLocationsRequest generateDeleteMitigationRequest(String name, int version) {
        return generateDeleteMitigationRequest(MitigationTemplate.BlackWatchPOP_PerTarget_EdgeCustomer, name, version);
    }

    public static MitigationDefinition defaultMitigationDefinition() {
        return createMitigationDefinition(PacketAttributesEnumMapping.DESTINATION_IP.name(), Lists.newArrayList("1.2.3.4"));
    }

    public static MitigationDefinition createMitigationDefinition(String attrName, List<String> attrValues) {
        BlackWatchConfigBasedConstraint constraint = new BlackWatchConfigBasedConstraint();
        S3Object s3object = new S3Object();
        s3object.setBucket("bucket");
        s3object.setKey("key");
        s3object.setMd5("md5");
        constraint.setConfig(s3object);
        MitigationDefinition definition = new MitigationDefinition();
        definition.setConstraint(constraint);
        return definition;
    }
}

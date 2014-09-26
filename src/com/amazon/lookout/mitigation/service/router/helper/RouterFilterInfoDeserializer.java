package com.amazon.lookout.mitigation.service.router.helper;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.CollectionUtils;

import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.mitigation.router.model.RouterFilterInfoWithMetadata;
import com.amazon.lookout.mitigation.router.model.RouterMitigationActionType;
import com.amazon.lookout.mitigation.service.ActionType;
import com.amazon.lookout.mitigation.service.CompositeAndConstraint;
import com.amazon.lookout.mitigation.service.CompositeOrConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.CountAction;
import com.amazon.lookout.mitigation.service.DropAction;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.RateLimitAction;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.DDBBasedRouterMetadataHelper;

/**
 * Helps to convert RouterFilterInfoWithMetadata into different models used by the MitigationService.
 *
 */
public class RouterFilterInfoDeserializer {
    
    /**
     * Helper method to convert an instance of RouterFilterInfoWithMetadata into an instance of MitigationDefinition.
     * @param filterInfo Instance of RouterFilterInfoWithMetadata using which we need to create a new MitigationDefinition
     * @return MitigationDefinition corresponding to the above RouterFilterInfoWithMetadata
     */
    public static MitigationDefinition convertToMitigationDefinition(RouterFilterInfoWithMetadata filterInfo) {
        MitigationDefinition definition = new MitigationDefinition();
        
        RouterMitigationActionType routerMitigationActionType = filterInfo.getAction();
        ActionType convertedActionType = null;
        switch(routerMitigationActionType) {
        case COUNT:
            convertedActionType = new CountAction();
            break;
        case DROP:
            convertedActionType = new DropAction();
            break;
        case RATE_LIMIT:
            convertedActionType = extractRateLimitAction(filterInfo);
            break;
        }
        definition.setAction(convertedActionType);
        
        Constraint constraint = extractConstraint(filterInfo);
        definition.setConstraint(constraint);
        
        return definition;
    }
    
    /**
     * Helper method to use RouterFilterInfoWithMetadata for creating a new instance of MitigationActionMetadata
     * @param filterInfo RouterFilterInfoWithMetadata to be used for creating MitigationActionMetadata
     * @return MitigationActionMetadata created using the RouterFilterInfoWithMetadata instance.
     */
    public static MitigationActionMetadata convertToActionMetadata(RouterFilterInfoWithMetadata filterInfo) {
        MitigationActionMetadata metadata = new MitigationActionMetadata();
        
        metadata.setDescription(filterInfo.getDescription());
        metadata.setToolName(DDBBasedRouterMetadataHelper.ROUTER_MITIGATION_UI);
        metadata.setUser(filterInfo.getLastUserToPush());
        
        return metadata;
    }
    
    private static RateLimitAction extractRateLimitAction(RouterFilterInfoWithMetadata filterInfo) {
        RateLimitAction actionType = new RateLimitAction();
        actionType.setRateLimitInKBps(filterInfo.getBandwidthKBps());
        actionType.setBurstSizeInKB(filterInfo.getBurstSizeK());
        return actionType;
    }
    
    /**
     * Converts an instance of RouterFilterInfoWithMetadata into an instance of Constraint model used by the MitigationService. 
     * @param filterInfo RouterFilterInfoWithMetadata to be used for creating Constraint
     * @return Constraint created using the RouterFilterInfoWithMetadata instance.
     */
    private static Constraint extractConstraint(RouterFilterInfoWithMetadata filterInfo) {
        List<Constraint> constraints = new ArrayList<>();
        
        if (!CollectionUtils.isEmpty(filterInfo.getDestIps())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_IP.name());
            constraint.setAttributeValues(filterInfo.getDestIps());
            constraints.add(constraint);
        }
        
        // Check for source constraints. If we have more than 1 source constraint, then we need to create a CompositeOrConstraint across all of them.
        List<Constraint> sourceConstraints = new ArrayList<>();
        if (!CollectionUtils.isEmpty(filterInfo.getSrcIps())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.SOURCE_IP.name());
            constraint.setAttributeValues(filterInfo.getSrcIps());
            sourceConstraints.add(constraint);
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getSrcASNs())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.SOURCE_ASN.name());
            constraint.setAttributeValues(filterInfo.getSrcASNs());
            sourceConstraints.add(constraint);
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getSrcCountryCodes())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.SOURCE_COUNTRY.name());
            constraint.setAttributeValues(filterInfo.getSrcCountryCodes());
            sourceConstraints.add(constraint);
        }
        
        if (!sourceConstraints.isEmpty()) {
            if (sourceConstraints.size() > 1) {
                CompositeOrConstraint orConstraint = new CompositeOrConstraint();
                orConstraint.setConstraints(sourceConstraints);
                constraints.add(orConstraint);
            } else {
                constraints.addAll(sourceConstraints);
            }
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getPacketLength())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.TOTAL_PACKET_LENGTH.name());
            constraint.setAttributeValues(filterInfo.getPacketLength());
            constraints.add(constraint);
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getTtl())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.TIME_TO_LIVE.name());
            constraint.setAttributeValues(filterInfo.getTtl());
            constraints.add(constraint);
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getSourcePort())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.SOURCE_PORT.name());
            constraint.setAttributeValues(filterInfo.getSourcePort());
            constraints.add(constraint);
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getDestinationPort())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.DESTINATION_PORT.name());
            constraint.setAttributeValues(filterInfo.getDestinationPort());
            constraints.add(constraint);
        }
        
        if (!CollectionUtils.isEmpty(filterInfo.getProtocols())) {
            SimpleConstraint constraint = new SimpleConstraint();
            constraint.setAttributeName(PacketAttributesEnumMapping.LAYER_4_PROTOCOL.name());
            List<String> protocolNumAsString = new ArrayList<>();
            for (Integer protocolNumber : filterInfo.getProtocols()) {
                protocolNumAsString.add(String.valueOf(protocolNumber));
            }
            constraint.setAttributeValues(protocolNumAsString);
            constraints.add(constraint);
        }
        
        if (constraints.isEmpty()) {
            throw new RuntimeException("Got no constraints for filter: " + filterInfo);
        }
        
        if (constraints.size() > 1) {
            CompositeAndConstraint compositeAndConstraint = new CompositeAndConstraint();
            compositeAndConstraint.setConstraints(constraints);
            return compositeAndConstraint;
        } else {
            return constraints.get(0);
        }
    }
}

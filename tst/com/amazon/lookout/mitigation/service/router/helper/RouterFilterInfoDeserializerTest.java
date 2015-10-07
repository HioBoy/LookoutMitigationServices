package com.amazon.lookout.mitigation.service.router.helper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazon.aws158.commons.packet.PacketAttributesEnumMapping;
import com.amazon.lookout.test.common.util.TestUtils;
import com.amazon.lookout.mitigation.router.model.RouterFilterInfoWithMetadata;
import com.amazon.lookout.mitigation.router.model.RouterMitigationActionType;
import com.amazon.lookout.mitigation.service.CompositeAndConstraint;
import com.amazon.lookout.mitigation.service.CompositeOrConstraint;
import com.amazon.lookout.mitigation.service.Constraint;
import com.amazon.lookout.mitigation.service.MitigationActionMetadata;
import com.amazon.lookout.mitigation.service.MitigationDefinition;
import com.amazon.lookout.mitigation.service.RateLimitAction;
import com.amazon.lookout.mitigation.service.SimpleConstraint;
import com.amazon.lookout.mitigation.service.activity.helper.DDBBasedRouterMetadataHelper;
import com.google.common.collect.Lists;

public class RouterFilterInfoDeserializerTest {

    @BeforeClass
    public static void setup() {
        TestUtils.configureLogging();
    }
    
    @Test
    public void testConvertToMitigationDefinition() {
        RouterFilterInfoWithMetadata filterInfo = new RouterFilterInfoWithMetadata();
        filterInfo.setAction(RouterMitigationActionType.RATE_LIMIT);
        filterInfo.setBandwidthKBps(1500);
        filterInfo.setBurstSizeK(15);
        
        List<String> destIPs = Lists.newArrayList("1.2.3.4", "2.3.4.5");
        filterInfo.setDestIps(destIPs);
        
        List<String> sourceIPs = Lists.newArrayList("10.20.30.40");
        filterInfo.setSrcIps(sourceIPs);
        
        List<String> sourceASNs = Lists.newArrayList("1234", "2345");
        filterInfo.setSrcASNs(sourceASNs);
        
        List<String> ttls = Lists.newArrayList("54", "60");
        filterInfo.setTtl(ttls);
        
        MitigationDefinition definition = RouterFilterInfoDeserializer.convertToMitigationDefinition(filterInfo);
        assertNotNull(definition);
        
        assertTrue(definition.getAction() instanceof RateLimitAction);
        
        RateLimitAction rateLimitAction = (RateLimitAction) definition.getAction();
        assertEquals(rateLimitAction.getRateLimitInKBps(), filterInfo.getBandwidthKBps());
        assertEquals(rateLimitAction.getBurstSizeInKB(), filterInfo.getBurstSizeK());
        
        Constraint convertedConstraint = definition.getConstraint();
        assertTrue(convertedConstraint instanceof CompositeAndConstraint);
        
        CompositeAndConstraint convertedAndConstraint = (CompositeAndConstraint) convertedConstraint;
        assertEquals(convertedAndConstraint.getConstraints().size(), 3);
        
        SimpleConstraint convertedDestIPConstraint = null;
        SimpleConstraint convertedTTLConstraint = null;
        CompositeOrConstraint convertedSourceConstraints = null;
        for (Constraint constraint : convertedAndConstraint.getConstraints()) {
            if (constraint instanceof SimpleConstraint) {
                if (((SimpleConstraint) constraint).getAttributeName().equals(PacketAttributesEnumMapping.DESTINATION_IP.name())) {
                    convertedDestIPConstraint = (SimpleConstraint) constraint;
                    continue;
                }
                
                if (((SimpleConstraint) constraint).getAttributeName().equals(PacketAttributesEnumMapping.TIME_TO_LIVE.name())) {
                    convertedTTLConstraint = (SimpleConstraint) constraint;
                    continue;
                }
            }
            
            if (constraint instanceof CompositeOrConstraint) {
                convertedSourceConstraints = (CompositeOrConstraint) constraint;
            }
        }
        assertNotNull(convertedDestIPConstraint);
        assertEquals(convertedDestIPConstraint.getAttributeValues(), destIPs);
        
        assertNotNull(convertedTTLConstraint);
        assertEquals(convertedTTLConstraint.getAttributeValues(), ttls);
        
        assertNotNull(convertedSourceConstraints);
        assertEquals(convertedSourceConstraints.getConstraints().size(), 2);
        
        SimpleConstraint convertedSourceIPConstraint = null;
        SimpleConstraint convertedSourceASNConstraint = null;
        for (Constraint sourceConstraint : convertedSourceConstraints.getConstraints()) {
            if (sourceConstraint instanceof SimpleConstraint) {
                if (((SimpleConstraint) sourceConstraint).getAttributeName().equals(PacketAttributesEnumMapping.SOURCE_IP.name())) {
                    convertedSourceIPConstraint = (SimpleConstraint) sourceConstraint;
                    continue;
                }
                
                if (((SimpleConstraint) sourceConstraint).getAttributeName().equals(PacketAttributesEnumMapping.SOURCE_ASN.name())) {
                    convertedSourceASNConstraint = (SimpleConstraint) sourceConstraint;
                }
            }
        }
        
        assertNotNull(convertedSourceIPConstraint);
        assertEquals(convertedSourceIPConstraint.getAttributeValues(), sourceIPs);
        
        assertNotNull(convertedSourceASNConstraint);
        assertEquals(convertedSourceASNConstraint.getAttributeValues(), sourceASNs);
    }
    
    @Test
    public void testConvertToActionMetadata() {
        RouterFilterInfoWithMetadata filterInfo = new RouterFilterInfoWithMetadata();
        filterInfo.setLastUserToPush("testUser");
        filterInfo.setDescription("Testing");
        MitigationActionMetadata metadata = RouterFilterInfoDeserializer.convertToActionMetadata(filterInfo);
        assertNotNull(metadata);
        assertEquals(metadata.getDescription(), filterInfo.getDescription());
        assertEquals(metadata.getToolName(), DDBBasedRouterMetadataHelper.ROUTER_MITIGATION_UI);
        assertEquals(metadata.getUser(), filterInfo.getLastUserToPush());
    }
}

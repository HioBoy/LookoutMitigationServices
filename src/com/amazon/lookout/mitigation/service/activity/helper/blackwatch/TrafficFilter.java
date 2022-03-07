package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;

import com.amazon.aws.authruntimeclient.internal.common.collect.ImmutableSet;
import com.amazon.aws.authruntimeclient.internal.common.collect.Sets;
import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * BlackWatch Traffic filter definition.
 * @author xingbow
 *
 */
@ToString
@EqualsAndHashCode
public class TrafficFilter {

    public enum AttributeType {
        IPv4_CIDR(NetworkCidr.class),
        PORT(PortRange.class),
        L4_PROTO(L4Protocol.class);

        @Getter
        private Class<?> type;
        private AttributeType(Class<?> type) {
            this.type = type;
        }
    }
    
    public enum AttributeName {
        SOURCE_IP(AttributeType.IPv4_CIDR),
        DEST_IP(AttributeType.IPv4_CIDR),
        SOURCE_PORT(AttributeType.PORT),
        DEST_PORT(AttributeType.PORT),
        L4_PROTOCOL(AttributeType.L4_PROTO);
        
        @Getter
        private AttributeType type;
        
        private AttributeName(AttributeType type) {
            this.type = type;
        }

    }
   
    @Getter
    private final Map<AttributeName, PacketAttribute> attributes;
    
    @Getter
    private Set<Integer> relatedOriginFilterIndices;

    private TrafficFilter(Map<AttributeName, PacketAttribute> attributes, Set<Integer> relatedOriginFilterIndices) {
        this.attributes = ImmutableMap.copyOf(attributes);
        this.relatedOriginFilterIndices = ImmutableSet.copyOf(relatedOriginFilterIndices);
        
        // Validate attribute name matches attribute
        for (Map.Entry<AttributeName, PacketAttribute> attributeEntry : this.attributes.entrySet()) {
            PacketAttribute attribute = attributeEntry.getValue();
            AttributeName attributeName = attributeEntry.getKey();
            Validate.isTrue(attributeName.getType().getType().isInstance(attribute),
                    String.format("AttributeName %s and Attribute %s does not match.", attributeName, attribute));
        }
    }
    
    /**
     * Create traffic filter from attributes, and a set of related origin filter indices
     * @param attributes
     * @param relatedOriginFilterIndices
     */
    public static TrafficFilter createFilter(Map<AttributeName, PacketAttribute> attributes, Set<Integer> relatedOriginFilterIndices) {
        return new TrafficFilter(attributes, relatedOriginFilterIndices);
    }
    
    /**
     * Create traffic filter from attributes, and a filter index
     * @param attributes
     * @param relatedOriginFilterIndices
     */
    public static TrafficFilter createFilter(Map<AttributeName, PacketAttribute> attributes, Integer index) {
        return new TrafficFilter(attributes, Sets.newHashSet(index));
    }
   
    /**
     * find overlap filter between current filter and another filter.
     * @param o : another filter
     * @return : overlap filter. return null, if not found.
     */
    public TrafficFilter findOverlap(TrafficFilter o) {
        Validate.notNull(o, "null parameter when findOverlap between Traffic filters");
        Map<AttributeName, PacketAttribute> overlapAttributes = new EnumMap<>(AttributeName.class);
        // put attributes from both traffic filter into overlap filter first. Order does not matter.
        // This covers the attributes that is only present in one of the filters.
        overlapAttributes.putAll(o.attributes);
        overlapAttributes.putAll(this.attributes);
        // find overlap packet attribute for the one that is shared by both filters.
        Set<AttributeName> overlapAttributeNameSet = Sets.newHashSet(this.attributes.keySet());
        overlapAttributeNameSet.retainAll(o.attributes.keySet());
        for (AttributeName attributeName : overlapAttributeNameSet) {
            PacketAttribute overlapAttribute = this.attributes.get(attributeName).findOverlap(o.attributes.get(attributeName));
            if (overlapAttribute != null){
                overlapAttributes.put(attributeName, overlapAttribute);
            } else {
                return null;
            }
        }
        Set<Integer> relatedOriginFilterIndices = new HashSet<>();
        relatedOriginFilterIndices.addAll(this.relatedOriginFilterIndices);
        relatedOriginFilterIndices.addAll(o.relatedOriginFilterIndices);
        return new TrafficFilter(overlapAttributes, relatedOriginFilterIndices);
    }

    /**
     * Add more related filter indices.
     * @param relatedOriginFilterIndices
     */
    public void addRelatedFilterIndices(Set<Integer> relatedOriginFilterIndices) {
        Validate.notNull(relatedOriginFilterIndices, "null parameter when addRelatedFilterIndices");
        if (!this.relatedOriginFilterIndices.containsAll(relatedOriginFilterIndices)) {
            this.relatedOriginFilterIndices = new ImmutableSet.Builder<Integer>()
                    .addAll(relatedOriginFilterIndices)
                    .addAll(this.relatedOriginFilterIndices).build();
        }
    }

    /**
     * Check whether this filter attributes definition is exactly same as another one
     * @param other : another filter
     * @return : true, if same, false if not
     */
    public boolean sameDefAs(TrafficFilter other) {
        return this.attributes.equals(other.getAttributes());
    }

}

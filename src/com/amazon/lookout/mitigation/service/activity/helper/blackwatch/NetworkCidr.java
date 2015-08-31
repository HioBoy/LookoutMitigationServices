package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.serializer.NetworkCidrJsonSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Range;

/**
 * Network IPV4 CIDR.
 * @author xingbow
 *
 */
@EqualsAndHashCode
@JsonSerialize(using = NetworkCidrJsonSerializer.class)
public class NetworkCidr implements PacketAttribute {
    public static final Pattern NETWORK_CIDR_PATTERN = Pattern.compile("\\s*(\\d{1,3}).(\\d{1,3}).(\\d{1,3}).(\\d{1,3})\\s*/\\s*(\\d{1,2})\\s*");
    private static final Range<Integer> IP_SEGMENT_VALID_RANGE = Range.closed(0, 255);
    private static final Range<Integer> MASK_DEPTH_VALID_RANGE = Range.closed(0, 32);
    
    @Getter
    private final int prefix;
    @Getter
    private final int mask;
    @Getter
    private final int depth;
    
    /**
     * construct NetworkCidr object from network CIDR string.
     * Example : "1.1.1.1/32",   "10.0.0.0/8"
     * @param cidr : CIDR string
     * @return NetworkCidr object
     */
    public static NetworkCidr fromString (String cidr) {
        Validate.notEmpty(cidr, "NetworkCidr String constructor parameter can not be null");
        
        Matcher m = NETWORK_CIDR_PATTERN.matcher(cidr);
        Validate.isTrue(m.matches(), String.format("input of NetworkCidr constructor %s does not match pattern %s", cidr, NETWORK_CIDR_PATTERN));
        
        int prefix = 0;
        for (int i = 1; i <= 4; ++i) {
            int segment = Integer.parseInt(m.group(i));
            Validate.isTrue(IP_SEGMENT_VALID_RANGE.contains(segment), String.format("IP address %s is invalid", cidr));
            prefix = (prefix << 8) | segment;
        }
        int depth = Integer.parseInt(m.group(5));
        
        return new NetworkCidr(prefix, depth);
    }
    
    private NetworkCidr(int prefix, int depth) {
        this.prefix = prefix;
        this.depth = depth;
        Validate.isTrue(MASK_DEPTH_VALID_RANGE.contains(depth), String.format("IPv4 CIDR depth %d is not in valid range %s", depth, MASK_DEPTH_VALID_RANGE));
        this.mask = (int)(0xffffffffL << (32 - depth));
        Validate.isTrue((prefix & mask) == prefix, "CIDR mask does not match prefix.");
    }
    
    /**
     * Find overlap network CIDR between this CIDR and another CIDR.
     * @param o : another network CIDR
     * @return overlap network CIDR, if found. null, if not found.
     */
    @Override
    public PacketAttribute findOverlap(PacketAttribute o) {
        Validate.isTrue(o instanceof NetworkCidr, 
                String.format("Cannot operate findOverlap method on non-NetworkCidr object : %s", o));
        NetworkCidr other = (NetworkCidr) o;
        
        int shortestMask = this.mask & other.mask;
        if ((shortestMask & this.prefix) == (shortestMask & other.prefix)) {
            // find overlap
            return new NetworkCidr((this.prefix | other.prefix), Math.max(this.depth, other.depth));
        } else {
            // no overlap
            return null;
        }
    }
   
    @Override
    public String toString() {
        return String.format("%d.%d.%d.%d/%d", (prefix >> 24) & 0xff , 
                (prefix >> 16) & 0xff, (prefix >> 8) & 0xff,
                prefix & 0xff, depth);
    }
}

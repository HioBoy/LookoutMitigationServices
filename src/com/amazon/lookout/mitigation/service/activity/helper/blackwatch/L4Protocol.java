package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.serializer.L4ProtocolJsonSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

/**
 * L4 protocol identifier attribute of traffic filter.
 * This protocol identifier concept is from DPDK ACL library.
 * http://dpdk.readthedocs.org/en/latest/sample_app_ug/l3_forward_access_ctrl.html
 * Quote from DPDK sample Application user guide:
 * "Protocol identifier : An 8-bit field, represented by a value and a mask, 
 * that covers a range of values. To verify that a value is in the range, 
 * use the following expression: “(VAL & mask) == value”
 * 
 * The trick in how to represent a range with a mask and value is as follows.
 * A range can be enumerated in binary numbers with some bits that are never 
 * changed and some bits that are dynamically changed. Set those bits that 
 * dynamically changed in mask and value with 0. Set those bits that never 
 * changed in the mask with 1, in value with number expected. For example, 
 * a range of 6 to 7 is enumerated as 0b110 and 0b111. Bit 1-7 are bits never 
 * changed and bit 0 is the bit dynamically changed. Therefore, set bit 0 in 
 * mask and value with 0, set bits 1-7 in mask with 1, and bits 1-7 in value 
 * with number 0b11. So, mask is 0xfe, value is 0x6."
 * 
 * This approach doesn't cover any arbitrary protocol range. 
 * In fact, its only generically useful for matching either all protocols
 * or one specific protocol in most of the cases.
 */
@EqualsAndHashCode
@JsonSerialize(using = L4ProtocolJsonSerializer.class)
public class L4Protocol implements PacketAttribute {
    @Getter
    private final int value;
    @Getter
    private final int mask;
    
    private static final Range<Integer> VALID_RANGE = Range.closed(0, 255);
    public static final Pattern L4_PROTOCOL_PATTERN = Pattern.compile("\\s*(\\d{1,3})\\s*/\\s*0x([0-9a-f]{1,2})\\s*");
    
    private L4Protocol(int value, int mask) {
        Validate.isTrue(VALID_RANGE.contains(value), String.format("L4 protocol value %d is not in valid range %s", value, VALID_RANGE));
        Validate.isTrue(VALID_RANGE.contains(mask), String.format("L4 protocol mask value 0x%x is not in valid range %s", mask, VALID_RANGE));
        Validate.isTrue(value == (value & mask), String.format("L4 protocol value %d does not match mask 0x%x.", value, mask));

        this.value = value;
        this.mask = mask;
    }
    
    private static enum Protocol{
        ICMP(1, 0xff), TCP(6, 0xff), UDP(17, 0xff), ALL(0, 0x00);
        
        @Getter
        private int value;
        @Getter
        private int mask;
        
        Protocol(int value, int mask) {
            this.value = value;
            this.mask = mask;
        }
    }
    
    private static final Map<String, Protocol> PREDEFINED_PROTOCOL; 
    static {
        Map<String, Protocol> nameToProtocol = new HashMap<>();
        for (Protocol protocol : Protocol.values()) {
            nameToProtocol.put(protocol.name().toUpperCase(), protocol);
        }
        PREDEFINED_PROTOCOL = ImmutableMap.copyOf(nameToProtocol);
    }
    
    /**
     * Construct L4 Protocol object from string.
     * It supports human readable string like "TCP", "UDP", "ICMP", "ALL",
     * It also support "value/mask", like "6/0xff", "0/0xff"
     * @param protocolStr
     * @return L4Protocol object
     */
    public static L4Protocol fromString(String protocolStr) {
        Validate.notEmpty(protocolStr, "L4Protocol constructor does not allow empty parameter");
        Protocol predefined_protocol = PREDEFINED_PROTOCOL.get(protocolStr.trim().toUpperCase());
        if (predefined_protocol != null) {
            return new L4Protocol(predefined_protocol.getValue(), predefined_protocol.getMask());
        }
        
        Matcher m = L4_PROTOCOL_PATTERN.matcher(protocolStr);
        Validate.isTrue(m.matches(), String.format("value %s does not match L4 protocol input pattern %s", protocolStr, L4_PROTOCOL_PATTERN));
        return new L4Protocol(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2), 16));
    }
    
    /**
     * Find overlap L4 protocol value, between this and another L4 protocol value.
     * @param o : another L4 protocol object
     * @return overlap L4 protocol. null if not found.
     */
    @Override
    public PacketAttribute findOverlap(PacketAttribute o) {
        Validate.isTrue(o instanceof L4Protocol, 
                String.format("Cannot operate findOverlap method on non-L4Protocol object : %s", o));
        L4Protocol other = (L4Protocol) o;
        int andMasks = this.mask & other.mask;
        if ((andMasks & this.value) == (andMasks & other.value)) {
            // found overlap
            return new L4Protocol(this.value | other.value, this.mask | other.mask);
        } else {
            // no overlap, same bit get masked, but have different value.
            return null;
        }
    }
    
    @Override
    public String toString() {
        return String.format("%d/0x%x", this.value, this.mask);
    }
}

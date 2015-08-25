package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.Validate;

import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.serializer.PortRangeJsonSerializer;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Range;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * Port range class handle UDP, TCP port range.
 * Range consists of lower end and upper end
 * @author xingbow
 *
 */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
@JsonSerialize(using = PortRangeJsonSerializer.class)
public class PortRange implements PacketAttribute {
    public static final Pattern PORT_RANGE_PATTERN = Pattern.compile("\\s*(\\d{1,5})\\s*:\\s*(\\d{1,5})\\s*");
    private static final Range<Integer> VALID_PORT_RANGE = Range.closed(0, 65535);
    private final Range<Integer> range;
    
    public int getLowerEnd() {
        return range.lowerEndpoint();
    }
    
    public int getUpperEnd() {
        return range.upperEndpoint();
    }
    
    private PortRange(int from, int to) {
        Validate.isTrue(VALID_PORT_RANGE.contains(from), String.format("port value from : %d, "
                    + "is not inside valid port range %s range.", from, VALID_PORT_RANGE));
        Validate.isTrue(VALID_PORT_RANGE.contains(to), String.format("port value from : %d, "
                    + "is not inside valid port range %s range.", to, VALID_PORT_RANGE));
 
        Validate.isTrue(from <= to, String.format("port range from : %d must be less than to : %d", from, to));
        range = Range.closed(from, to);
    }
    
    /**
     * construct port range from string.
     * The string has to been a continuous range.
     * Example : "0:65535",  "1024:2022"
     * @param portRange : port range string, match pattern 
     * @return
     */
    public static PortRange fromString(String portRange) {
        Matcher m = PORT_RANGE_PATTERN.matcher(portRange);
        Validate.isTrue(m.matches(), String.format("port range value %s does not match input regex %s", portRange, PORT_RANGE_PATTERN));
        return new PortRange(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
    }
    
    /**
     * Find overlap port range between this port ranges, and another port range
     * @param o : another port range
     * @return overlap PortRange. return null, if not found.
     */
    @Override
    public PacketAttribute findOverlap(PacketAttribute o) {
        Validate.isTrue(o instanceof PortRange, String.format("Can not operate findOverlap method on non NetworkCidr object : %s", o));
        PortRange other = (PortRange) o;
        if (this.range.isConnected(other.range)) {
            return new PortRange(this.range.intersection(other.range));
        } else {
            return null;
        }
    }
    
    @Override
    public String toString() {
        return String.format("%d:%d", range.lowerEndpoint(), range.upperEndpoint());
    }
}

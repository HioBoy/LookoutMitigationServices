package com.amazon.lookout.mitigation.service.activity.helper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.owasp.esapi.codecs.HashTrie;

import com.amazon.aws158.commons.customer.CustomerSubnetsFetcher;
import com.amazon.aws158.commons.net.IPUtils;

/**
 * ServiceSubnetsMatcher maintains a trie of subnets to service mapping.
 * It is responsible to help clients figure out if a subnet or a list of subnets belong to a single service.
 *
 */
@ThreadSafe
public class ServiceSubnetsMatcher {
    private static final Log LOG = LogFactory.getLog(ServiceSubnetsMatcher.class);
    
    private static final String SUBNET_MASK_DELIMITER = "/";
    
    private class ServiceSubnet {
        private final String serviceName;
        private final String subnet;
        private final int maskLength;
        
        public ServiceSubnet(@Nonnull String serviceName, @Nonnull String subnet, @Nonnull int maskLength) {
            Validate.notEmpty(serviceName);
            Validate.notEmpty(subnet);
            Validate.isTrue((maskLength > 0) && (maskLength <= 32));
            
            this.serviceName = serviceName;
            this.subnet = subnet;
            this.maskLength = maskLength;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getSubnet() {
            return subnet;
        }

        public int getMaskLength() {
            return maskLength;
        }
    }
    
    private final HashTrie<ServiceSubnet> validServiceSubnetsTrie;
    
    public ServiceSubnetsMatcher(@Nonnull List<CustomerSubnetsFetcher> serviceSubnetsFetcher) {
        Validate.notEmpty(serviceSubnetsFetcher);
        
        Map<String, List<String>> serviceSubnetsMap = new HashMap<>();
        for (CustomerSubnetsFetcher subnetsFetcher : serviceSubnetsFetcher) {
            serviceSubnetsMap.put(subnetsFetcher.getCustomerName(), subnetsFetcher.getAllSubnets());
        }
        
        LOG.debug("Creating ServiceSubnetsMatcher for serviceSubnets: " + serviceSubnetsMap);
        validServiceSubnetsTrie = buildServiceSubnetsTrie(serviceSubnetsMap);
    }
    
    private HashTrie<ServiceSubnet> buildServiceSubnetsTrie(Map<String, List<String>> serviceSubnetsMap) {
        HashTrie<ServiceSubnet> serviceSubnetsTrie = new HashTrie<>();
        
        // maintaining a mapping of cidr string to ServiceSubnet. We do this to avoid the case where
        // services might have the same cidrString but with different mask lengths - in which
        // case we want to only persist the service whose subnet is most generic (smaller mask length).
        // We could have done this by searching before inserting into the validServiceSubnetsTrie and deleting the
        // entry with a larger mask length - unfortunately currently (03/2014) the OWASP HashTrie we 
        // use doesn't support the remove operation.
        Map<String, ServiceSubnet> cidrAsStringToServiceSubnet = new HashMap<>();
        
        for (Map.Entry<String, List<String>> serviceSubnets : serviceSubnetsMap.entrySet()) {
            String serviceName = serviceSubnets.getKey();
            
            for (String subnet : serviceSubnets.getValue()) {
                // Obtain the ip address in the subnet mask as an int.
                String[] subnetParts = subnet.split(SUBNET_MASK_DELIMITER);
                int ipAddrAsInt = IPUtils.ipAsInt(subnetParts[0]);
                int maskLength = Integer.parseInt(subnetParts[1]);
                ServiceSubnet serviceSubnet = new ServiceSubnet(serviceName, subnet, maskLength);
                
                String cidrAsString = StringUtils.leftPad(Integer.toBinaryString(ipAddrAsInt), IPUtils.NUM_BITS_IN_IPV4, '0');
                
                if (cidrAsStringToServiceSubnet.containsKey(cidrAsString)) {
                    ServiceSubnet overlappingServiceSubnet = cidrAsStringToServiceSubnet.get(cidrAsString);
                    if (overlappingServiceSubnet.getMaskLength() < maskLength) {
                        // If this service subnet has a longer mask length than a previous one with the same 
                        // cidr but smaller mask length, then we skip this entry to always prefer the more generic subnet.
                        continue;
                    }
                }
                cidrAsStringToServiceSubnet.put(cidrAsString, serviceSubnet);
            }
        }
        
        for (Map.Entry<String, ServiceSubnet> entry : cidrAsStringToServiceSubnet.entrySet()) {
            String cidrAsString = entry.getKey();
            ServiceSubnet serviceSubnet = entry.getValue();
            int maskLength = serviceSubnet.getMaskLength();
            serviceSubnetsTrie.put(cidrAsString.substring(0, maskLength), serviceSubnet);
        }
        
        return serviceSubnetsTrie;
    }
    
    /**
     * Compare the input subnet against subnets belonging to different services, to identify which service's subnet(s) might be equal or 
     * a super-set of the input subnet. Return serviceName if such a service is found, null otherwise.
     * @param subnetToCheck Subnet to check against the service's subnets.
     * @return String representing serviceName for the service whose subnet matches (or is a super-set) the input subnet. Null otherwise.
     */
    public String getServiceForSubnet(@Nonnull String subnetToCheck) {
        Validate.notEmpty(subnetToCheck);
        
        String matchedServiceName = null;
        
        String ipAddress = subnetToCheck;
        int maskLengthToCheck = 32;
        if (subnetToCheck.contains(SUBNET_MASK_DELIMITER)) {
            String[] subnetParts = subnetToCheck.split(SUBNET_MASK_DELIMITER);
            ipAddress = subnetParts[0];
            maskLengthToCheck = Integer.parseInt(subnetParts[1]);
        }
        
        int ipAsInt = IPUtils.ipAsInt(ipAddress);
        
        // Get the ipAddress to match as a String representing the bits in the address. We leftPad with 0s to make the length 32 bits.
        String ipAsString = StringUtils.leftPad(Integer.toBinaryString(ipAsInt), IPUtils.NUM_BITS_IN_IPV4, '0');
        Entry<CharSequence, ServiceSubnet> match = validServiceSubnetsTrie.getLongestMatch(ipAsString);
        
        if (match != null) {
            int matchedServiceSubnetMaskLength = match.getValue().getMaskLength();
            
            // If the matched subset has maskLength greater than the maskLength of the subnet being checked, then we ignore
            // that service, since the subnet being checked is broader than the match. Since we prefer generic subnets in 
            // the trie in the cases of overlap (eg: 1.2.3.0/24 vs 1.2.3.0/25, we would keep 1.2.3.0/24), if the best match had 
            // a larger maskLength, we wouldn't have any better match.
            if (matchedServiceSubnetMaskLength <= maskLengthToCheck) {
                matchedServiceName = match.getValue().getServiceName();
            }
        }
        LOG.debug("SubnetToCheck: " + subnetToCheck + " matched service: " + matchedServiceName);
        return matchedServiceName;
    }
    
    /**
     * Compare the input subnets against subnets belonging to different services, to identify which service's subnet(s) might be equal or 
     * a super-set for all the input subnets. Return serviceName if such a service is found, null otherwise.
     * @param subnetsToCheck List of subnets to check against service's subnets.
     * @return String representing serviceName for the service whose subnets matches (or is a super-set) the input subnets. Null otherwise.
     */
    public String getServiceForSubnets(@Nonnull List<String> subnetsToCheck) {
        Validate.notEmpty(subnetsToCheck);
        
        String serviceForAllSubnets = null;
        
        for (String subnet : subnetsToCheck) {
            String serviceForCurrentSubnet = getServiceForSubnet(subnet);

            // If we don't find any service to whom this subnet might belong, we simply return null
            if (serviceForCurrentSubnet == null) {
                return null;
            }
            
            // If the subnets belong to different services, in that case too then we simply return null since
            if ((serviceForAllSubnets != null) && (!serviceForAllSubnets.equals(serviceForCurrentSubnet))) {
                return null;
            } else {
                serviceForAllSubnets = serviceForCurrentSubnet;
            }
        }

        LOG.debug("SubnetsToCheck: " + subnetsToCheck + " matched service: " + serviceForAllSubnets);
        return serviceForAllSubnets;
    }
}

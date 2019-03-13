package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import com.aws.rip.RIPHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aws.rip.RIPHelper;
import com.aws.rip.models.exception.RegionIsInTestException;
import com.aws.rip.models.exception.RegionNotFoundException;
import com.aws.rip.models.region.Region;
import com.aws.rip.models.constants.AccessibilityAttribute;

public class Utils {
    private static final Log LOG = LogFactory.getLog(Utils.class);

    private static final String LOOKOUT_PREFIXES_METADATA_FORMAT = "com.amazon.lookout.prefixes.metadata.reader.%s";
    private static final String LOOKOUT_PREFIXES_METADATA_REGIONAL_FORMAT = "com.amazon.lookout.prefixes.metadata.reader.%s.%s";

    public static Boolean isAWSOptInRegion(String region) {
        try {
            Region reg = RIPHelper.getRegion(region);
            List<AccessibilityAttribute> attrs = reg.getAccessibilityAttributes();
            String regionAttrs = attrs.stream().map(a -> a.toString()).collect(Collectors.joining(" ,"));
            LOG.info(String.format("Region %s has the following accessibility attributes: %s", region, regionAttrs));
            return attrs.contains(AccessibilityAttribute.NO_GLOBAL_CREDS);
        } catch (Exception ex) {
            String msg = String.format("Caught exception when querying region %s", region);
            LOG.error(msg, ex);
        }

        return false;
    }


    public static String getMetadataMaterialSet(String domain, String region, Boolean isAWSOptinRegion) {
        String ms = isAWSOptinRegion ? String.format(LOOKOUT_PREFIXES_METADATA_REGIONAL_FORMAT, domain, region) :
                String.format(LOOKOUT_PREFIXES_METADATA_FORMAT, domain);
        LOG.info(String.format("Lookout material set for region %s domain %s is '%s'", region, domain, ms));
        return ms;
    }

}

package com.amazon.lookout.mitigation.service.activity.helper.blackwatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.Validate;
import com.amazon.lookout.mitigation.service.activity.helper.blackwatch.TrafficFilter.AttributeName;

/**
 * This class extracts overlap traffic filters from a set of input overlap filters.
 * 
 * @author xingbow
 *
 */
public class OverlapTrafficFiltersExtractor {
    /**
     * Create list of traffic filters from origin attributes list
     */
    private static List<TrafficFilter> createTrafficFiltes(List<Map<AttributeName, PacketAttribute>> originAttributesList) {
        Validate.notEmpty(originAttributesList);
        // check uniqueness
        Set<Map<AttributeName, PacketAttribute>> attributesSet = new HashSet<>();
        List<TrafficFilter> originFilters = new ArrayList<>(originAttributesList.size());
        // assign index of origin filter
        int index = 0;
        for (Map<AttributeName, PacketAttribute> attributes : originAttributesList) {
            Validate.isTrue(!attributesSet.contains(attributes), 
                String.format("Duplicate attributes in originAttributesList param. Attributes : %s", attributes));
            originFilters.add(TrafficFilter.createFilter(attributes, index++));
            attributesSet.add(attributes);
        }
        return originFilters;
    }
    
    /**
     * perform filter extraction algorithm.
     * The basic idea is to iterate origin filters to find out all the overlap filters.
     * E.g. if the origin filter list include filter A, B, C, and they overlap each other with unique overlap filter definitions.
     * The algorithm will perform this way:
     * 1. process filter A,
     *    extracted filter list : A
     * 2. process filter B,
     *    extracted filter list : AB, A, B
     * 3. process filter C,
     *    extracted filter list : ABC, AB, AC, A, BC, B, C
     *    
     * More details can be found in unit test.
     * @param originAttributesList: list of origin attributes map
     * @return : list of extracted overlap TrafficFilters
     */
    public static List<TrafficFilter> extractOverlapFilters(List<Map<AttributeName, PacketAttribute>> originAttributesList) {
        List<TrafficFilter> originFilters = createTrafficFiltes(originAttributesList);
        // extracted filter list
        LinkedList<TrafficFilter> extractedFilterList = new LinkedList<>();
        // keep track of the filter we have already found
        Map<Map<AttributeName, PacketAttribute>, TrafficFilter> foundFilterMap = new HashMap<>();
        
        for (TrafficFilter originFilter : originFilters) {
            boolean shouldInsertOriginFilter = true;
            ListIterator<TrafficFilter> it = extractedFilterList.listIterator();
            while(it.hasNext()) {
                TrafficFilter curFilter = it.next();
                // if current filter is same as origin filter, then add this origin filter 
                // as the related origin filter of the current filter.
                // Also there is no need to continue process, and all the subset of current filter
                // has been processed before this filter, so we can finish the process of this origin filter
                if (curFilter.sameDefAs(originFilter)) {
                    curFilter.addRelatedFilterIndices(originFilter.getRelatedOriginFilterIndices());
                    shouldInsertOriginFilter = false;
                    break;
                } else {
                    TrafficFilter overlapFilter = curFilter.findOverlap(originFilter);
                    if (overlapFilter != null) {
                        TrafficFilter existingFilter = foundFilterMap.get(overlapFilter.getAttributes());
                        // if the overlap filter has already been found in the filterMap before,
                        // Add overlap filter's related origin filter list to the existing filter.
                        if (existingFilter != null) {
                            existingFilter.addRelatedFilterIndices(overlapFilter.getRelatedOriginFilterIndices());
                        } else {
                            // if this is a new filter, insert it before the current filter, and add it to the found filter Map
                            it.previous();
                            it.add(overlapFilter);
                            it.next();
                            foundFilterMap.put(overlapFilter.getAttributes(), overlapFilter);
                        }
                        // if the overlap filter is same as origin filter. we can stop processing this origin filter.
                        // this is similar as when origin filter is same as current filter.
                        if (overlapFilter.sameDefAs(originFilter)) {
                            shouldInsertOriginFilter = false;
                            break;
                        }
                    }
                }
            }
            if (shouldInsertOriginFilter) {
                // insert origin filter to the tail of the extracted filter list
                extractedFilterList.offer(originFilter);
                foundFilterMap.put(originFilter.getAttributes(), originFilter);
            }
        }
        
        return extractedFilterList;
    }
}

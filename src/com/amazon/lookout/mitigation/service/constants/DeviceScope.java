package com.amazon.lookout.mitigation.service.constants;

/**
 * This enum lists the different scopes that the devices supported by this service might operate in.
 * There could possibly be a case where a new scope is defined which might be a combination of 2 or more other scopes (eg: NorthAmerica = {US, CA}).
 * 
 * Each DeviceScope is also associated with a range of workflowIds, which should be in steps of million.
 * Having this range allows us to ensure that mitigations for the same device but different scopes don't overlap in their workflowIds.
 * The minimum workflowId value for each DeviceScope is the value that each DeviceName+DeviceScope would be initialized with in the DynamoDBTable.
 * 
 * Having the min and max specified here allows us to have a sanity check in the code to ensure the workflowIds returned back from DDB is within
 * the expected range.
 * 
 */
public enum DeviceScope {
    GLOBAL(0, 1000000);
    
    private long minWorkflowId;
    private long maxWorkflowId;
    
    private DeviceScope(long minWorkflowId, long maxWorkflowId) {
        this.minWorkflowId = minWorkflowId;
        this.maxWorkflowId = maxWorkflowId;
    }
    
    public long getMinWorkflowId() {
        return minWorkflowId;
    }
    
    public long getMaxWorkflowId() {
        return maxWorkflowId;
    }
}

package com.amazon.lookout.mitigation.service.activity.helper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * wrap request storage response data.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestStorageResponse {
    long workflowId;
    int mitigationVersion;
}

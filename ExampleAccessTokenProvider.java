/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This software is licensed with the Universal Permissive License (UPL) version 1.0
 *
 * Please see SAMPLELICENSE.txt file included in the top-level directory of the
 * appropriate download for a copy of the license and additional information.
 */

package oracle.nosql.cloudsim.examples;

import oracle.nosql.driver.idcs.AccessTokenProvider;

/**
 * A provider produces dummy access token using given tenant Id as value.
 * Without specifying tenant id, <code>TestTenant</code> will be used.
 */
public class ExampleAccessTokenProvider extends AccessTokenProvider {
    private String tenantId = "TestTenant";

    public ExampleAccessTokenProvider(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getTenantId() {
        return tenantId;
    }

    @Override
    public String getAccountAccessToken() {
        return tenantId;
    }

    @Override
    public String getServiceAccessToken() {
        return tenantId;
    }
}

/*-
 * Copyright (C) 2011, 2018 Oracle and/or its affiliates. All rights reserved.
 *
 * This software is licensed with the Universal Permissive License (UPL) version 1.0
 *
 * Please see SAMPLELICENSE.txt file included in the top-level directory of the
 * appropriate download for a copy of the license and additional information.
 */

package oracle.nosql.cloudsim.examples;

import java.net.URL;
import java.util.List;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.idcs.AccessTokenProvider;
import oracle.nosql.driver.idcs.DefaultAccessTokenProvider;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;

/**
 * A simple program to:
 *   1. connect to a running proxy or Oracle NoSQL Database Cloud Service
 *   2. drop a table
 *
 * Run against local Cloud Simulator:
 *
 * Assumes there is a running Cloud Simulator instance listening on port 8080 on
 * the local host. If non-default host or port are desired, use flags to
 * change them.
 *
 * Run against Oracle NoSQL Database Cloud Service:
 *
 * Requires an Oracle Cloud account with a subscription to the Oracle NoSQL
 * Database Cloud Service
 *
 * After that is done this information is required:
 *  o cloud account user name and password
 *  o client id and secret
 *  o IDCS URL assigned to the tenancy
 *  o entitlement id
 *
 * Obtain client id and secret from Oracle Identity Cloud Service (IDCS) admin
 * console, choose Applications from the button on the top left. Find the
 * Application named ANDC. The client id and secret are in the General
 * Information of Configuration. And then create the credential file at home
 * directory $HOME/.andc/credentials with these properties:
 * andc_username=<cloud account user name>
 * andc_user_pwd=<cloud account user password>
 * andc_client_id=<client id>
 * andc_client_secret=<client_secret>
 *
 * In addition the IDCS URL and entitlement ID are required to run the example,
 * or any application using the service.
 *
 * The tenant-specific IDCS URL is the IDCS host assigned to the tenant.  After
 * logging into the IDCS admin console, copy the host of the IDCS admin console
 * URL. For example, the format of the admin console URL is
 * "https://{tenantId}.identity.oraclecloud.com/ui/v1/adminconsole". The
 * "https://{tenantId}.identity.oraclecloud.com" portion is the required.
 *
 * The entitlement id can be found using the IDCS admin console. After logging
 * into the IDCS admin console, choose Applications from the button on
 * the top left. Find the Application named ANDC, enter the Resources tab in
 * the Configuration. There is a field called primary audience, the entitlement
 * id parameter is the value of "urn:opc:andc:entitlementid", which is treated
 * as a string. For example if your primary audience is
 * "urn:opc:andc:entitlementid=123456789" then the parameter is "123456789"
 */
public class DropTableExample {
    private static final String dropTableStatement =
        "drop table if exists ittage";

    private static final String PORT_FLAG = "-httpPort";
    private static final String HOST_FLAG = "-host";

    private static final String IDCS_FLAG = "-idcsUrl";
    private static final String ENTITLEMENT_FLAG = "-entitlementId";
    private static final String ANS_URL = "ans.uscom-east-1.oraclecloud.com";

    /* change to false to quiet output */
    private static final boolean verbose = true;

    private static void usage() {
        System.err.println(
            "Usage: Basic Table Operation Example\n" +
            "Run against CloudSim:\n" +
            "-httpPort <port> (default: 8080)\n" +
            "-host <hostname> (default: localhost)\n\n" +
            "Run against NoSQL Cloud Service\n" +
            "-idcsUrl <idcsUrl> Tenant Identity Cloud Service URL \n" +
            "-entitlementId <entitlementId> The entitlement id of ANS");
        System.exit(1);
    }

    private static void missingRequireArgs(String name) {
        System.err.println("Missing required argument " + name);
        usage();
    }

    @SuppressWarnings("unused")
    public static void main(String[] args) throws Exception {

        /*
         * Defaults
         */
        final String tenantId = "mytenantId";
        String hostname = "localhost";
        int port = 8080;
        String idcsUrl = null;
        String entitlementId = null;

        if (args.length > 0) {
            if (args.length != 2 && args.length != 4) {
                usage();
            }
            for (int i = 0; i < args.length; i += 2) {
                if (PORT_FLAG.equals(args[i])) {
                    port = Integer.parseInt(args[i+1]);
                } else if (HOST_FLAG.equals(args[i])) {
                    hostname = args[i+1];
                } else if (IDCS_FLAG.equals(args[i])) {
                    idcsUrl = args[i+1];
                } else if (ENTITLEMENT_FLAG.equals(args[i])) {
                    entitlementId = args[i+1];
                } else {
                    usage();
                }
            }
        }

        if (idcsUrl != null && entitlementId == null) {
            missingRequireArgs(ENTITLEMENT_FLAG);
        }
        if (idcsUrl == null && entitlementId != null) {
            missingRequireArgs(IDCS_FLAG);
        }

        URL serviceURL = null;
        AccessTokenProvider atProvider = null;
        if (idcsUrl == null) {
            output("Using host " + hostname + ", port " + port);
            serviceURL = new URL("http", hostname, port, "/");
            atProvider = new ExampleAccessTokenProvider(tenantId);
        } else {
            output("Running against " + ANS_URL);
            serviceURL = new URL("https", ANS_URL, 443, "/");
            atProvider = new DefaultAccessTokenProvider(entitlementId, idcsUrl);
        }

        /*
         * Configure the endpoint and set the tenant id
         */
        NoSQLHandleConfig config = new NoSQLHandleConfig(serviceURL);
        config.setAuthorizationProvider(atProvider);

        /*
         * Open the handle
         */
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);

        try {

            /*
             * Drop a table
             */
            TableRequest tableRequest = new TableRequest()
                .setStatement(dropTableStatement)
                .setTableLimits(new TableLimits(600, 300, 5));
            TableResult tres = handle.tableRequest(tableRequest);

            /*
             * Wait for the table to become active. Table request is asynchronous. It's necessary
             * to wait for an expected state to know when the operation has completed.
             */
            tres = TableResult.waitForState(handle,
                                            tres.getTableName(),
                                            TableResult.State.ACTIVE,
                                            60000, /* wait 60 sec */
                                            1000); /* delay ms for poll */

            output("DropTableExample: table dropped using statement:\n\t" +
                   dropTableStatement);

		} catch (NoSQLException nse) {
            System.err.println("Op failed: " + nse.getMessage());
        } catch (Exception e) {
            System.err.println("Exception processing msg: " + e);
            e.printStackTrace();
        } finally {
            /*
             * Shutdown client or process won't exit
             */
            output("DropTableExample: closing handle");
            handle.close();
        }
    }

    private static void output(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }
}

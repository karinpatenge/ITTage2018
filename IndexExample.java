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
 * 1. connect to a running proxy
 * 2. create a table
 * 3. put a row
 * 4. Index Create
 * 5. read from Index
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
public class IndexExample {
    private static final String createTableStatement =
        "create table if not exists usersInfo(id integer, "
        + "userInfo JSON, primary key(id))";

    private static final String PORT_FLAG = "-httpPort";
    private static final String HOST_FLAG = "-host";

    private static final String IDCS_FLAG = "-idcsUrl";
    private static final String ENTITLEMENT_FLAG = "-entitlementId";
    private static final String ANS_URL = "ans.uscom-east-1.oraclecloud.com";

    /* change to false to quiet output */
    private static final boolean verbose = true;

    private static void usage() {
        System.err.println(
            "Usage: Index Operation Example\n" +
            "Run against CloudSim:\n" +
            "-httpPort <port> (default: 8080)\n" +
            "-host <hostname> (defualt: localhost)\n\n" +
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
        String entitleId = null;

        if (args.length > 0) {
            if (args.length != 2 && args.length != 4) {
                usage();
            }
            for (int i = 0; i < args.length; i += 2) {
                if (PORT_FLAG.equals(args[i])) {
                    port = Integer.parseInt(args[i + 1]);
                } else if (HOST_FLAG.equals(args[i])) {
                    hostname = args[i + 1];
                } else if (IDCS_FLAG.equals(args[i])) {
                    idcsUrl = args[i+1];
                } else if (ENTITLEMENT_FLAG.equals(args[i])) {
                    entitleId = args[i+1];
                } else {
                    usage();
                }
            }
        }

        if (idcsUrl != null && entitleId == null) {
            missingRequireArgs(ENTITLEMENT_FLAG);
        }
        if (idcsUrl == null && entitleId != null) {
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
            atProvider = new DefaultAccessTokenProvider(entitleId, idcsUrl);
        }

        /*
         * Configure the endpoint and set the access token provider
         */
        NoSQLHandleConfig config = new NoSQLHandleConfig(serviceURL);
        config.setAuthorizationProvider(atProvider);

        /*
         * Open the handle
         */
        NoSQLHandle handle =
            NoSQLHandleFactory.createNoSQLHandle(config);

        try {

            /*
             * Create a simple table with
             * an integer key and a single name field
             */
            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement)
                .setTableLimits(
                    new TableLimits(500, 500, 500));
            TableResult tres = handle.tableRequest(tableRequest);

            /*
             * Wait for the table to become active. Table request
             * is asynchronous. It's necessary to wait for an
             * expected state to know when the operation has
             * completed.
             */
            tres = TableResult.waitForState(
                handle,
                tres.getTableName(),
                TableResult.State.ACTIVE, 30000, /* wait 30 sec */
                1000); /* delay ms for poll */

            output("Index Create example: created table, "
                   + "statement:\n\t"
                   + createTableStatement);

            final String index1 = "CREATE INDEX idx1 on usersInfo"
                + "(userInfo.firstName as string)";

            TableRequest indexTableRequest = new TableRequest()
                .setStatement(index1)
                .setTableLimits(
                    new TableLimits(500, 500, 500));
            TableResult indexTableResult =
                handle.tableRequest(tableRequest);
            /*
             * PUT a row
             */

            /* construct a simple row */
            MapValue value = new MapValue().put("id", 1).
                putFromJson("userInfo",
                            "{\"firstName\":\"myname\","
                            + "\"lastName\":\"mylastName\","
                            + "\"age\":33}",null);

            PutRequest putRequest =
                new PutRequest().setValue(value)
                .setTableName("usersInfo");

            PutResult putRes = handle.put(putRequest);
            output("Index Create example: put row: " + value);

            /* construct a simple row */
            value = new MapValue().put("id", 2).
                putFromJson("userInfo",
                            "{\"firstName\":\"newname\","
                            + "\"lastName\":\"mynewName\","
                            + "\"age\":35}",null);

            // no options
            putRequest = new PutRequest().setValue(value)
                .setTableName("usersInfo");

            putRes = handle.put(putRequest);
            output("Index Create example: put row: " + value);

            /* construct a row */
            value = new MapValue().put("id", 3).
                putFromJson("userInfo",
                            "{\"firstName\":\"friendsname\","
                            + "\"lastName\":\"friendslastName\","
                            + "\"age\":30}",null);

            // no options
            putRequest = new PutRequest().setValue(value)
                .setTableName("usersInfo");

            putRes = handle.put(putRequest);
            output("Index Create example: put row: " + value);

            /* construct a row */
            value = new MapValue().put("id", 4).
                putFromJson("userInfo",
                            "{\"firstName\":\"relativesname\","
                            + "\"lastName\":\"relativeslastName\","
                            + "\"age\":43}",null);

            // no options
            putRequest = new PutRequest().setValue(value)
                .setTableName("usersInfo");

            putRes = handle.put(putRequest);
            output("Index Create example: put row: " + value);

            /*
             * Query from an index
             */
            QueryRequest queryRequest =
                new QueryRequest().setStatement(
                    "select * from usersInfo u where "
                    + "u.userInfo.firstName=\"myname\"");
            output("Query :"+queryRequest.getStatement());
            QueryResult qres = handle.query(queryRequest);
            List<MapValue> results = qres.getResults();
            output("Index Query Example: number of query results: "
                   + results.size());
            for (MapValue qval : qres.getResults()) {
                output("\t" + qval.toString());
            }
        } catch (NoSQLException nse) {
            System.err.println("Op failed: " + nse.getMessage());
        } catch (Exception e) {
            System.err.println("Exception processing msg: " + e);
            e.printStackTrace();
        } finally {
            /*
             * Shutdown client or process won't exit
             */
            output("Index Create example: closing handle");
            handle.close();
        }
    }

    private static void output(String msg) {
        if (verbose) {
            System.out.println(msg);
        }
    }
}

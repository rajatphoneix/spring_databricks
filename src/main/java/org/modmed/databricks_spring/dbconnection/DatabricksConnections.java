package org.modmed.databricks_spring.dbconnection;

import java.sql.*;
import java.util.Properties;

public class DatabricksConnections {
//https://dbc-2faf3e73-a44f.cloud.databricks.com/?o=4401295611745102
//
//
//    dapi061ae2bd5d9d44750d582723ccd5f584 PAT
//
// 419877469953631 Job ID
//
//    in databricks go to sql datawarehouse to get the connection details
//    it will show the db connection string and also generate token button to generate a token
//    dapieb7041eab52387373513f5d52001252d
//    copy the snippet code, I have changed it to Connection and return connection
//    in PWD(line 22 replace with token, if you have a program already
    public static Connection connectToDatabricksSQLWarehouse() {
        String url = "jdbc:databricks://dbc-a7fd220b-9975.cloud.databricks.com:443;HttpPath=/sql/1.0/warehouses/9a5e1f394f4c7588";
        Properties properties = new Properties();
        properties.put("PWD", "dapi061ae2bd5d9d44750d582723ccd5f584");
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            if (connection != null) {
                return connection;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return null;
    }
}

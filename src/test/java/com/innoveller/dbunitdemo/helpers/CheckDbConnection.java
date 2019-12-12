package com.innoveller.dbunitdemo.helpers;

import com.innoveller.dbunitdemo.helpers.DataSourceHelper;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.xml.FlatXmlWriter;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class CheckDbConnection {
    public static void main(String[] args) throws SQLException, DatabaseUnitException, IOException {
        //fetchStudents();
        //useIDbConnection();
    }

    public static void fetchHotels() throws SQLException, IOException {
        DataSource dataSource = DataSourceHelper.buildDataSource();
        Connection connection = dataSource.getConnection();
        Statement stmt = null;
        String query = "select * from hotel";
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                Long id = rs.getLong("id");
                String name = rs.getString("name");
                System.out.println(id + "\t" + name);            }
        } catch (SQLException e ) {
            e.printStackTrace();
        } finally {
            if (stmt != null) { stmt.close(); }
        }
        connection.close();
    }

    public static void useIDbConnection() throws SQLException, DatabaseUnitException, IOException {
        DataSource dataSource = DataSourceHelper.buildDataSource();
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);
        //ITable table = dbConn.createTable("student");
        QueryDataSet partialDataSet = new QueryDataSet(dbConn);
        //Specify the SQL to run to retrieve the data
        partialDataSet.addTable("all_students", " SELECT * FROM student");

        FlatXmlWriter writer = new FlatXmlWriter(new FileOutputStream("results/temp.xml"));
        writer.write(partialDataSet);
    }
}

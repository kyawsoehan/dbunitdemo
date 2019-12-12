package com.innoveller.dbunitdemo.helpers;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DataSourceHelper {
    private static class DbConfig {
        String host;
        String dbName;
        String user;
        String password;
    }

    public static DataSource buildDataSource() throws IOException {
        DbConfig dbConfig = loadDbConfig();

        PGSimpleDataSource ds = new PGSimpleDataSource() ;  // Empty instance.
        ds.setServerName(dbConfig.host);
        ds.setDatabaseName(dbConfig.dbName);
        ds.setUser(dbConfig.user);
        ds.setPassword(dbConfig.password);
        return ds;
    }

    public static DbConfig loadDbConfig() throws IOException {
        InputStream input = new FileInputStream("db-config.properties");
        Properties prop = new Properties();
        // load a properties file
        prop.load(input);

        DbConfig dbConfig = new DbConfig();
        dbConfig.host = prop.getProperty("db.host");
        dbConfig.dbName = prop.getProperty("db.name");
        dbConfig.user = prop.getProperty("db.user");
        dbConfig.password = prop.getProperty("db.password");
        return dbConfig;
    }
}

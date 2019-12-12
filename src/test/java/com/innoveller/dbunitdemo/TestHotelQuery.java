package com.innoveller.dbunitdemo;

import com.innoveller.dbunitdemo.helpers.DataSourceHelper;
import com.ninja_squad.dbsetup.DbSetup;
import com.ninja_squad.dbsetup.destination.DataSourceDestination;
import com.ninja_squad.dbsetup.operation.Operation;
import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.sql.Connection;

import static com.ninja_squad.dbsetup.Operations.*;

public class TestHotelQuery {
    private static DataSource dataSource;

    @BeforeAll
    public static void prepare() throws Exception {
        //https://stackoverflow.com/questions/45091981/produce-a-datasource-object-for-postgres-jdbc-programmatically
        System.out.println("Preparing DBSetup...");
        dataSource = DataSourceHelper.buildDataSource();
        Operation operation =
                sequenceOf(
                        deleteAllFrom("hotel", "room_type"),
                        //CommonOperations.INSERT_REFERENCE_DATA,
                        insertInto("hotel")
                                .columns("id", "name")
                                .values(1L, "Yangon Hotel")
                                .values(2L, "Mandalay Inn")
                                .build(),
                        insertInto("room_type")
                                .columns("hotel_id", "name")
                                .values(1L, "Standard Room")
                                .values(2L, "King Room")
                                .values(2L, "Queen Room")
                                .build()
                );
        DbSetup dbSetup = new DbSetup(new DataSourceDestination(dataSource), operation);
        dbSetup.launch();
        System.out.println("Launched DBSetup operations.");
    }

    @Test
    void testHotelRoomTypeCount() throws Exception {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        ITable resultTable = dbConn.createQueryTable("result",
                "SELECT hotel.id as hotel_id, hotel.name as hotel_name, COUNT(room_type.id) as num_room_types from hotel\n" +
                        "RIGHT JOIN room_type on hotel.id = room_type.hotel_id\n" +
                        "GROUP BY hotel.id\n" +
                        "ORDER BY hotel.id");

        Column[] columns = new Column[] {
                new Column("hotel_id", DataType.INTEGER),
                new Column("hotel_name", DataType.VARCHAR),
                new Column("num_room_types", DataType.BIGINT) //or UNKNOWN
        };
        DefaultTable expectedTable = new DefaultTable("expected", columns);
        expectedTable.addRow(new Object[] {1, "Yangon Hotel", 1});
        expectedTable.addRow(new Object[] {2, "Mandalay Inn", 2});

        Assertion.assertEquals(resultTable, expectedTable);
    }
}

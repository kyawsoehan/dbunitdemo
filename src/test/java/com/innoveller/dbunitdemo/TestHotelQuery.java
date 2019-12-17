package com.innoveller.dbunitdemo;

import com.innoveller.dbunitdemo.helpers.TableBuilder;
import com.innoveller.dbunitdemo.helpers.DataSourceHelper;
import com.innoveller.dbunitdemo.services.QueryBuilderService;
import com.ninja_squad.dbsetup.DbSetup;
import com.ninja_squad.dbsetup.destination.DataSourceDestination;
import com.ninja_squad.dbsetup.operation.Operation;
import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;

import static com.ninja_squad.dbsetup.Operations.*;

public class TestHotelQuery {
    private static DataSource dataSource;
    private QueryBuilderService queryBuilderService = new QueryBuilderService();

    @BeforeAll
    public static void prepare() throws Exception {
        //https://stackoverflow.com/questions/45091981/produce-a-datasource-object-for-postgres-jdbc-programmatically
        System.out.println("Preparing DBSetup...");
        dataSource = DataSourceHelper.buildDataSource();
        Operation operation =
                sequenceOf(
                        deleteAllFrom("room_type_date_allotment", "room_type_image",
                                "rate_group_date_rate", "room_type_extra_bed_rate", "rate_group", "room_type",
                                "hotel_to_attraction", "attraction", "hotel_image", "hotel", "township", "town"),
                        //CommonOperations.INSERT_REFERENCE_DATA,
                        insertInto("town")
                                .columns("id", "name_en")
                                .values(1L, "Yangon")
                                .values(2L, "Mandalay")
                                .build(),
                        insertInto("township")
                                .columns("id", "name_en", "town_id")
                                .values(1L, "Kamayut", 1L)
                                .values(2L, "Chan Aye Thar Zan", 2L)
                                .build(),
                        insertInto("hotel")
                                .columns("id", "code", "name", "town_id", "township_id")
                                .values(1L, "yangon-hotel", "Yangon Hotel", 1L, 1L)
                                .values(2L, "mandalay-inn", "Mandalay Inn", 2L, 2L)
                                .build(),
                        insertInto("room_type")
                                .columns("id", "hotel_id", "name", "max_adults_with_extra_bed", "max_guests_with_extra_bed")
                                .values(1L, 1L, "Standard Room", 3, 3)
                                .values(2L, 2L, "King Room", 4, 4)
                                .values(3L, 2L, "Queen Room", 5, 5)
                                .build(),
                        insertInto("room_type_date_allotment")
                                .columns("room_type_id", "date", "allotment")
                                .values(1L, "2019-12-25", 20)
                                .values(1L, "2019-12-26", 20)
                                .values(1L, "2019-12-27", 20)
                                .values(1L, "2019-12-28", 20)
                                .values(1L, "2019-12-29", 20)
                                .values(1L, "2019-12-30", 20)
                                .build(),
                        insertInto("room_type_image")
                                .columns("room_type_id", "image_url")
                                .values(1L, "image1.png")
                                .values(1L, "image2.png")
                                .build(),
                        insertInto("rate_group")
                                .columns("id", "guest_type", "room_type_id", "minimum_advance_days", "maximum_advance_days",
                                        "is_active", "based_on_plan_id", "additional_percentage")
                                .values(1L, "default", 1L, null, null, true, null, null)
                                .values(2L, "local", 1L, 15, 20, true, 1L, -10.0)
                                .build(),
                        insertInto("rate_group_date_rate")
                                .columns("rate_group_id", "date", "rate")
                                .values(1L, "2019-12-25", 10000)
                                .values(1L, "2019-12-26", 10000)
                                .values(1L, "2019-12-27", 10000)
                                .build(),
                        insertInto("room_type_extra_bed_rate")
                                .columns("room_type_id", "rate", "rate_group_id")
                                .values(1L, 2000, 1L)
                                .build(),
                        insertInto("attraction")
                                .columns("id", "name_en", "town_id")
                                .values(1L, "People's Park", 1L)
                                .values(2L, "U Bein Bridge", 2L)
                                .build(),
                        insertInto("hotel_to_attraction")
                                .columns("id", "hotel_id", "attraction_id")
                                .values(1L, 1L, 1L)
                                .values(2L, 2L, 2L)
                                .build(),
                        insertInto("hotel_image")
                                .columns("id", "hotel_id", "image_url")
                                .values(1L, 1L, "image1.png")
                                .values(2L, 2L, "image2.png")
                                .build()

                );
        DbSetup dbSetup = new DbSetup(new DataSourceDestination(dataSource), operation);
        dbSetup.launch();

        DataSource dataSource = DataSourceHelper.buildDataSource();
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement("REFRESH MATERIALIZED VIEW hotel_summary;");
        statement.executeUpdate();
        connection.close();

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

        org.dbunit.dataset.Column[] columns = new org.dbunit.dataset.Column[] {
                new org.dbunit.dataset.Column("hotel_id", DataType.INTEGER),
                new org.dbunit.dataset.Column("hotel_name", DataType.VARCHAR),
                new org.dbunit.dataset.Column("num_room_types", DataType.BIGINT) //or UNKNOWN
        };
        /*DefaultTable expectedTable = new DefaultTable("expected", columns1);
        expectedTable.addRow(new Object[] {1, "Yangon Hotel", 1});
        expectedTable.addRow(new Object[] {2, "Mandalay Inn", 2});
        */

        //TableBuilder.columns()
        org.dbunit.dataset.Column[] columns2 = TableBuilder.columns(columns).buildColumns();

        org.dbunit.dataset.Column[] columns1 = TableBuilder.columns()
                .add("hotel_id", DataType.INTEGER)
                .add("hotel_name", DataType.VARCHAR)
                .add("num_room_types", DataType.BIGINT)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns2)
                .add(1, "Yangon Hotel", 1)
                .add(2, "Mandalay Inn", 2)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testAvailableRoomTypeIdQuery() throws Exception {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        //String sqlString = queryBuilderService.getAvailableRoomTypeId(startDate,endDate,1,1,1,1);

        String sqlString = queryBuilderService.getAvailableRoomTypeId(startDate,endDate,1,1,1,1);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("room_type_id", DataType.INTEGER)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testAvailableRoomTypeIdAndImagesQuery() throws Exception {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getAvailableRoomTypeIdNImages(startDate,endDate,1,1,1,1);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("room_type_id", DataType.INTEGER)
                .add("image_urls", DataType.VARCHAR)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, "image1.png<>image2.png")
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testAvailableRoomTypeInfo() throws Exception {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getAvailableRoomTypeInfo(startDate,endDate,1,1,1,1);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("id", DataType.INTEGER)
                .add("name", DataType.VARCHAR)
                .add("number_of_adult", DataType.INTEGER)
                .add("number_of_guest", DataType.INTEGER)
                .add("image_urls", DataType.VARCHAR)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, "Standard Room", 3, 3, "image1.png<>image2.png")
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testAvailableRoomTypeRates() throws Exception {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getAvailableRoomTypeRates(startDate,endDate,1,"local");

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("room_type_id", DataType.INTEGER)
                .add("rate_group_id", DataType.INTEGER)
                .add("date_rates", DataType.VARCHAR)
                .add("extra_bed_rate", DataType.NUMERIC)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, 1, "2019-12-25:10000.00,0.0<>2019-12-26:10000.00,0.0", 2000.00)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

   /* @Test
    void testFileReader () throws Exception{
        String string = fileHelper.readSql
                ("C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\hello.txt");

        System.out.println(string);
    }

    @Test
    void testGetHotel () throws Exception{
        String sqlString = fileHelper.readSql
                ("C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\getHotel.sql");


        String code = "yangon-hotel";
        sqlString = sqlString.replace(":code", code);

        System.out.println(sqlString);
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("id", DataType.INTEGER)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }
*/

   @Test
    void testHotelsByLocation() throws SQLException, DatabaseUnitException, IOException {
       Connection connection = dataSource.getConnection();
       IDatabaseConnection dbConn = new DatabaseConnection(connection);

       String sqlString = queryBuilderService.getHotelsByLocation("town", 1L);

       ITable resultTable = dbConn.createQueryTable("result", sqlString);

       //id, code, name, town_id, town_name_en, image_urls, township_id, township_name_en, attractions
       org.dbunit.dataset.Column[] columns = TableBuilder.columns()
               .add("id", DataType.INTEGER)
               .add("code", DataType.VARCHAR)
               .add("name", DataType.VARCHAR)
               .add("town_id", DataType.INTEGER)
               .add("town_name_en", DataType.VARCHAR)
               .add("image_urls", DataType.VARCHAR)
               .add("township_id", DataType.INTEGER)
               .add("township_name_en", DataType.VARCHAR)
               .add("attractions", DataType.UNKNOWN)
               .buildColumns();

       DefaultTable defaultTable = TableBuilder.rows("expected", columns)
               .add(1, "yangon-hotel", "Yangon Hotel", 1, "Yangon", "image1.png", 1, "Kamayut", "{1}")
               .buildTable();

       Assertion.assertEquals(defaultTable, resultTable);
   }

    @Test
    void testHotelAndRoomType() throws SQLException, DatabaseUnitException, IOException {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getHotelAndRoomType("town", 1L, 1, 1, 1, startDate, endDate);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("hotel_id", DataType.INTEGER)
                .add("room_type_id", DataType.INTEGER)
                .add("total_available", DataType.BIGINT)
                .add("rate_group_id", DataType.INTEGER)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, 1, 20, 1)
                .add(1, 1, 20, 2)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testHotelAndRates() throws SQLException, DatabaseUnitException, IOException {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getHotelAndRates("town", 1L, 1, 1, 1, startDate, endDate);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("hotel_id", DataType.INTEGER)
                .add("max_rate", DataType.NUMERIC)
                .add("min_rate", DataType.NUMERIC)
                .add("total_rate", DataType.NUMERIC)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, 10000.00, 10000.00, 20000.0)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testHotelAndRateUnique() throws SQLException, DatabaseUnitException, IOException {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getHotelAndRateUnique("town", 1L, 1, 1, 1, startDate, endDate);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("hotel_id", DataType.INTEGER)
                .add("min_standard_rate", DataType.NUMERIC)
                .add("max_standard_rate", DataType.NUMERIC)
                .add("total_standard_rate", DataType.NUMERIC)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, 10000.00, 10000.00, 20000.0)
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }

    @Test
    void testAvailableHotel() throws SQLException, DatabaseUnitException, IOException {
        Connection connection = dataSource.getConnection();
        IDatabaseConnection dbConn = new DatabaseConnection(connection);

        LocalDate startDate = LocalDate.parse("2019-12-25");
        LocalDate endDate = LocalDate.parse("2019-12-26");
        String sqlString = queryBuilderService.getHotel("town", 1L, 1, 1, 1, startDate, endDate);

        ITable resultTable = dbConn.createQueryTable("result", sqlString);

        org.dbunit.dataset.Column[] columns = TableBuilder.columns()
                .add("hotel_id", DataType.INTEGER)
                .add("min_standard_rate", DataType.NUMERIC)
                .add("max_standard_rate", DataType.NUMERIC)
                .add("total_standard_rate", DataType.NUMERIC)
                .add("code", DataType.VARCHAR)
                .add("name", DataType.VARCHAR)
                .add("town_id", DataType.INTEGER)
                .add("town_name_en", DataType.VARCHAR)
                .add("image_urls", DataType.VARCHAR)
                .add("township_id", DataType.INTEGER)
                .add("township_name_en", DataType.VARCHAR)
                .add("attractions", DataType.UNKNOWN)
                .buildColumns();

        DefaultTable defaultTable = TableBuilder.rows("expected", columns)
                .add(1, 10000.00, 10000.00, 20000.0, "yangon-hotel", "Yangon Hotel", 1, "Yangon", "image1.png", 1, "Kamayut", "{1}")
                .buildTable();

        Assertion.assertEquals(defaultTable, resultTable);
    }
}
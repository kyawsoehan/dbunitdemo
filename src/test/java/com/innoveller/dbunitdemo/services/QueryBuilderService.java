package com.innoveller.dbunitdemo.services;

import com.innoveller.dbunitdemo.helpers.FileReaderHelper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.Period;

public class QueryBuilderService {
    private FileReaderHelper fileReaderHelper = new FileReaderHelper();

    private String buildAvailableRoomTypeQuery(String pathName, String testName, LocalDate startDate, LocalDate endDate, int numOfRoom, long hotelId,
                                               int numOfAdult, int numOfGuest) throws IOException {
        String sqlString = fileReaderHelper.readSql(pathName, testName);

        Period period = Period.between(startDate, endDate);
        int totalDays = period.getDays() + 1;

        sqlString = sqlString.replace(":startDate", startDate + "");
        sqlString = sqlString.replace(":endDate", endDate + "");
        sqlString = sqlString.replace(":numOfRoom", numOfRoom + "");
        sqlString = sqlString.replace(":hotelId", hotelId + "");
        sqlString = sqlString.replace(":numOfAdult", numOfAdult + "");
        sqlString = sqlString.replace(":numOfGuest", numOfGuest + "");
        sqlString = sqlString.replace(":totalDays", totalDays + "");

        return sqlString;
    }



    public String getAvailableRoomTypeId (LocalDate startDate, LocalDate endDate, int numOfRoom, long hotelId,
                                          int numOfAdult, int numOfGuest) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableRoomTypeInfo.sql";
        return buildAvailableRoomTypeQuery(pathName, "getRoomTypeId", startDate, endDate, numOfRoom, hotelId, numOfAdult, numOfGuest);
    }

    public String getAvailableRoomTypeIdNImages (LocalDate startDate, LocalDate endDate, int numOfRoom, long hotelId,
    int numOfAdult, int numOfGuest) throws IOException {

        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableRoomTypeInfo.sql";
        return buildAvailableRoomTypeQuery(pathName, "getRoomTypeNImages", startDate, endDate, numOfRoom, hotelId, numOfAdult, numOfGuest);
    }

    public String getAvailableRoomTypeInfo(LocalDate startDate, LocalDate endDate, int numOfRoom, long hotelId,
                                           int numOfAdult, int numOfGuest) throws IOException {

        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableRoomTypeInfo.sql";
        return buildAvailableRoomTypeQuery(pathName, "getRoomTypeInfo", startDate, endDate, numOfRoom, hotelId, numOfAdult, numOfGuest);
    }

    public String getAvailableRoomTypeRates(LocalDate startDate, LocalDate endDate, long roomTypeId, String guestType) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableRoomTypeRates.sql";
        String sqlString = fileReaderHelper.readSql(pathName, "getRoomTypeRates");

        LocalDate date = LocalDate.now();
        Period periodBtwNowStart = Period.between(date,startDate);
        int advanceDays = periodBtwNowStart.getDays() + 1;

        sqlString = sqlString.replace(":startDate", startDate+"");
        sqlString = sqlString.replace(":endDate", endDate+"");
        sqlString = sqlString.replace(":roomTypeId", roomTypeId+"");
        sqlString = sqlString.replace(":guestType", guestType);
        sqlString = sqlString.replace(":advanceDays", advanceDays+ "");

        return sqlString;
    }

    public String getHotelsByLocation (String locationType, long locationId) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableHotel.sql";
        String sqlString = fileReaderHelper.readSql(pathName, "getHotelsByLocation");

        sqlString = sqlString.replace(":locationType", locationType);
        sqlString = sqlString.replace(":locationId", locationId + "");

        return sqlString;
    }

    private String buildAvailableHotelQuery(String pathName, String testName, String locationType, long locationId, int numOfAdult, int numOfGuest, int numOfRoom,
                                            LocalDate startDate, LocalDate endDate) throws IOException {

        String sqlString = fileReaderHelper.readSql(pathName, testName);
        Period period = Period.between(startDate, endDate);
        int totalDays = period.getDays() + 1;

        sqlString = sqlString.replace(":locationType", locationType);
        sqlString = sqlString.replace(":locationId", locationId + "");
        sqlString = sqlString.replace(":numOfAdult", numOfAdult + "");
        sqlString = sqlString.replace(":numOfGuest", numOfGuest + "");
        sqlString = sqlString.replace(":numOfRoom", numOfRoom + "");
        sqlString = sqlString.replace(":startDate", startDate + "");
        sqlString = sqlString.replace(":endDate", endDate + "");
        sqlString = sqlString.replace(":totalDays", totalDays + "");

        return sqlString;
    }

    public String getHotelAndRoomType(String locationType, long locationId, int numOfAdult, int numOfGuest, int numOfRoom,
                                      LocalDate startDate, LocalDate endDate) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableHotel.sql";
        return buildAvailableHotelQuery(pathName, "getHotelsNRoomType", locationType, locationId, numOfAdult, numOfGuest, numOfRoom, startDate, endDate);
    }

    public String getHotelAndRates(String locationType, long locationId, int numOfAdult, int numOfGuest, int numOfRoom,
                                      LocalDate startDate, LocalDate endDate) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableHotel.sql";
        return buildAvailableHotelQuery(pathName, "getHotelNRates", locationType, locationId, numOfAdult, numOfGuest, numOfRoom, startDate, endDate);
    }

    public String getHotelAndRateUnique(String locationType, long locationId, int numOfAdult, int numOfGuest, int numOfRoom,
                                   LocalDate startDate, LocalDate endDate) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableHotel.sql";
        return buildAvailableHotelQuery(pathName, "getUniqueHotelNRates", locationType, locationId, numOfAdult, numOfGuest, numOfRoom, startDate, endDate);
    }

    public String getHotel(String locationType, long locationId, int numOfAdult, int numOfGuest, int numOfRoom,
                                        LocalDate startDate, LocalDate endDate) throws IOException {
        String pathName = "C:\\Users\\DELL\\IdeaProjects\\dbunitdemo\\src\\test\\java\\com\\innoveller\\dbunitdemo\\sqlqueries\\availableHotel.sql";
        return buildAvailableHotelQuery(pathName, "getAvailableHotels", locationType, locationId, numOfAdult, numOfGuest, numOfRoom, startDate, endDate);
    }
}

package com.example.myRecordShopApp;/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: <2618508>
 *
 */

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
//import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import java.net.Socket;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.


public class RecordsDatabaseService extends Thread {

    private Socket serviceSocket = null;
    private String[] requestStr = new String[2]; //One slot for artist's name and one for recordshop's name.
    private ResultSet outcome = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL = Credentials.URL;


    //Class constructor
    public RecordsDatabaseService(Socket aSocket) {
        serviceSocket = aSocket;
        this.start();
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest() {
        this.requestStr[0] = ""; //For artist
        this.requestStr[1] = ""; //For recordshop

        String tmp = "";
        try {
            InputStream socketStream = this.serviceSocket.getInputStream();
            InputStreamReader socketReader = new InputStreamReader(socketStream);
            StringBuffer stringBuffer = new StringBuffer();
            char x;
            while (true)
            {
                System.out.println("Service thread: reading characters ");
                x = (char) socketReader.read();
                System.out.println("Service thread: " + x);
                if (x == '#') {
                    break;
                }
                stringBuffer.append(x);
            }


            System.out.println(stringBuffer);
            String[] parts = stringBuffer.toString().split(";", 2); // Ensure splitting into at most 2 parts
            if (parts.length == 2) {
                this.requestStr[0] = parts[0];
                this.requestStr[1] = parts[1];
            } else {
                System.out.println("Invalid request format.");
            }

        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }


    public boolean attendRequest() {
        boolean flagRequestAttended = true;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        this.outcome = null;
        String artist_lastname = this.requestStr[0];
        String record_shop = this.requestStr[1];

        String sql = "SELECT \n" +
                "    record.title,\n" +
                "    record.label,\n" +
                "    genre.name AS genre,\n" +
                "    record.rrp,\n" +
                "    COUNT(recordcopy.copyID) AS numCopies\n" +
                "FROM \n" +
                "    record\n" +
                "INNER JOIN \n" +
                "    genre ON record.genre = genre.name\n" +
                "INNER JOIN \n" +
                "    artist ON record.artistID = artist.artistID\n" +
                "INNER JOIN \n" +
                "    recordcopy ON record.recordID = recordcopy.recordID\n" +
                "INNER JOIN \n" +
                "    recordshop ON recordcopy.recordshopID = recordshop.recordshopID\n" +
                "WHERE \n" +
                "    artist.lastname = ?\n" +
                "    AND recordshop.city = ?\n" +
                "GROUP BY \n" +
                "    record.title, record.label, genre.name, record.rrp\n" +
                "HAVING COUNT(recordcopy.copyID) > 0;";

        try {
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);

            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, artist_lastname);
            preparedStatement.setString(2, record_shop);

            resultSet = preparedStatement.executeQuery();

            // Process the results
            while (resultSet.next()) {
                System.out.println(resultSet.getString("title") + " | " +
                        resultSet.getString("label") + " | " +
                        resultSet.getString("genre") + " | " +
                        resultSet.getString("rrp") + " | " +
                        resultSet.getInt("numCopies"));
            }

            // Optionally convert ResultSet to CachedRowSet for further processing if needed
            resultSet = preparedStatement.executeQuery();

            RowSetFactory factory = RowSetProvider.newFactory();
            CachedRowSet cachedRowSet = factory.createCachedRowSet();
            cachedRowSet.populate(resultSet);
            this.outcome = cachedRowSet;

        } catch (Exception e) {
            e.printStackTrace();
            flagRequestAttended = false; // Flag the request as not successfully attended in case of exceptions
        } finally {
            // Clean up resources
            try {
                if (resultSet != null) resultSet.close();
                if (preparedStatement != null) preparedStatement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                System.out.println("SQL exception in cleanup: " + e.getMessage());
            }
        }

        return flagRequestAttended;
    }

//
//    public boolean attendRequest() {
//        boolean flagRequestAttended = true;
//        Connection connection = null;
//        PreparedStatement preparedStatement = null;
//        ResultSet resultSet = null;
//        ResultSet resultSetForTerminal = null;
//
//        this.outcome = null;
////        StringTokenizer st1 = new StringTokenizer(requestStr);
////        String operator = st1.nextToken();
//        String artist_lastname = this.requestStr[0];
//        String record_shop = this.requestStr[1];
////        String sql = "SELECT * FROM artist WHERE lastname = '" + artist_lastname + "';"; //TO BE COMPLETED- Update this line as needed.
//        String sql = "SELECT \n" +
//                "    record.title,\n" +
//                "    record.label,\n" +
//                "    genre.name AS genre,\n" +
//                "    record.rrp,\n" +
//                "    COUNT(recordcopy.copyID) AS numCopies\n" +
//                "FROM \n" +
//                "    record\n" +
//                "INNER JOIN \n" +
//                "    genre ON record.genre = genre.name\n" +
//                "INNER JOIN \n" +
//                "    artist ON record.artistID = artist.artistID\n" +
//                "INNER JOIN \n" +
//                "    recordcopy ON record.recordID = recordcopy.recordID\n" +
//                "INNER JOIN \n" +
//                "    recordshop ON recordcopy.recordshopID = recordshop.recordshopID\n" +
//                "WHERE \n" +
//                "    artist.lastname = '" + artist_lastname + "'\n" +
//                "    AND recordshop.city = '" + record_shop + "'\n" +
//                "GROUP BY \n" +
//                "    record.title, record.label, genre.name, record.rrp;";
////                "    AND recordshop.city = '" + record_shop + "'\n" +
////               System.out.println(sql);
//
//        try {
//            //Connect to the database
//            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
//            //TO BE COMPLETED
//
//            //Make the query
//            preparedStatement = connection.prepareStatement(sql);
////            preparedStatement.setString(1, artist_lastname);
////            preparedStatement.setString(2, record_shop);
//            resultSet = preparedStatement.executeQuery();
//            //TO BE COMPLETED
//
//            while (resultSet.next()) {
//                System.out.println(resultSet.getString("title") + " | " +
//                        resultSet.getString("label") + " | " +
//                        resultSet.getString("genre") + " | " +
//                        resultSet.getString("rrp") + " | " +
//                        resultSet.getInt("numCopies"));
//            }
//
//            resultSet = preparedStatement.executeQuery();
//
//            //Process query
//            RowSetFactory factory = RowSetProvider.newFactory();
//            CachedRowSet cachedRowSet = factory.createCachedRowSet();
//            cachedRowSet.populate(resultSet);
//            this.outcome = cachedRowSet;
//            //TO BE COMPLETED -  Watch out! You may need to reset the iterator of the row set.
//
//            //Clean up
//            //TO BE COMPLETED
//
//        } catch (Exception e) {
////            System.out.println(e);
//            e.printStackTrace();
//        } finally {
//            //Clean up resources
//            try {
//                if (resultSet != null) resultSet.close();
//                if (preparedStatement != null) preparedStatement.close();
//                if (connection != null) connection.close();
//            } catch (SQLException e) {
//                System.out.println("SQL exception in cleanup: " + e.getMessage());
//            }
//        }
//
//        return flagRequestAttended;
//    }


    //Wrap and return service outcome
    public void returnServiceOutcome() {
        ObjectOutputStream objectOutputStream = null;
        try {

            OutputStream outputStream = this.serviceSocket.getOutputStream();
            objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(this.outcome);
            objectOutputStream.flush();
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);


        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        } finally {

            try {
                if (objectOutputStream != null) {
                    objectOutputStream.close();
                }
                this.serviceSocket.close();
            } catch (IOException e) {
                System.out.println("Service thread " + this.getId() + ": Error closing stream or socket - " + e);
            }
        }
    }


    //The service thread run() method
    public void run() {
        try {
            System.out.println("\n============================================\n");

            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            boolean tmp = this.attendRequest();


            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        } catch (Exception e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbworker;

import java.sql.*;
import java.util.Scanner;


/**
 *
 * @author azbus
 */
public class DBWorker {

    /**
     * @param args the command line arguments
     */
    static Connection con;
    static Statement stmt;
    static ResultSet rs;

    public static void main(String[] args) {

        Scanner scn = new Scanner(System.in);

        loadDriver();
        makeConnection();
        int choose = 0;
        while (true) {
            System.out.print("Please, input: \n0 - for exit;\n1 - for starting to input query;\n2 - for insert row in DB\n3 - for creating table\n4 - for drop table");
            choose = scn.nextInt();
            System.out.println();

            if (choose == 0) {
                closeAll();
                System.exit(choose);
            }
            if (choose == 1) {
                System.out.print("Please, input query: ");
                String query = scn.next();
                System.out.println();
                retrieveData(query);
            }
            if (choose == 2) {
                System.out.print("Please, input name of table. Example: GOODS\n");
                String table = scn.next();
                System.out.println();
                System.out.print("Please, input data, which you want insert. Example: 'eggs', 1, 2 \n");
                String row = scn.next();

                //insertData("'eggs'","GOODS");
                System.out.flush();
                insertData(row, table);

            }
            if (choose == 3) {
                System.out.print("Please, input name of table: ");
                String table = scn.next();
                System.out.println();

                System.out.print("Please, input names of columns and type of data, which you want add in new table. Example: NAME VARCHAR2(200), SUP_ID NUMBER\n");
                String columns = scn.next();
                System.out.println();
                createTable(table, columns);
            }
            if (choose == 4) {
                System.out.print("Please, input name of table: ");
                String table = scn.next();
                System.out.println();

                dropTable(table);
            }


        }
        
    }

    static void loadDriver() {
        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");
            //Class.forName("com.sybase.jdbc.SybDriver");
        } catch (java.lang.ClassNotFoundException e) {
            System.err.print("ClassNotFoundException: ");
            System.err.println(e.getMessage());
        }
    }

    // make a connection  step 3: establish connection
    static void makeConnection() {

        //for one of Oracle drivers
        //step 2: Define connection URL
        String host = "127.0.0.1";
        String dbName = "xe";
        //String dbName = "localhost1";
        int port = 1521;
        String oracleURL = "jdbc:oracle:thin:@" + host
                + ":" + port + ":" + dbName;
        //for how to set up data source name see below.
        try {
            con = DriverManager.getConnection(oracleURL, "azbus", "951753ser");
        } catch (SQLException ex) {
            System.err.println("database connection: " + ex.getMessage());
        }
    }

    //create a table
    static void createTable(String table, String columns) {
        String createString = "create table " + table + "(" + columns + ")";

        try {
            //step 4: create a statement
            stmt = con.createStatement();
            //step 5: execute a query or update.
            stmt.execute("drop table " + table);//if exists, drop it, get new one
            stmt.executeUpdate(createString);

        } catch (SQLException ex) {
            System.err.println("CreateTable: " + ex.getMessage());
        }
    }

    /**
     * Insert row to the table
     *
     * @param row data, which should be insert to the table. Example: INSERT
     * INTO COFFEES VALUES ('Colombian', 101, 8, 0, 0)
     * @param table it's the necessary table
     */
    public static void insertData(String row, String table) {
        try {
            stmt = con.createStatement();
            stmt.executeQuery("INSERT INTO " + table + " VALUES (" + row + ")");

        } catch (SQLException ex) {
            System.err.println("InsertData: " + ex.getMessage());
        }
    }

    public static void dropTable(String table) {
        String query = "DROP TABLE " + table + ";";
        try {
            stmt = con.createStatement();

            rs = stmt.executeQuery(query);

        } catch (SQLException ex) {
            System.err.println("DeleteTable: " + ex.getMessage());
        }

    }

    public static void retrieveData(String query) {
        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                int columnNum = rsmd.getColumnCount();
                int column = 1;
                while (column <= columnNum) {
                    String s = rs.getString(column);
                    column++;

                    System.out.print(s + "\t");
                }
                System.out.println();
            }
        } catch (SQLException ex) {
            System.err.println("RetrieveData: " + ex.getMessage());
        }
    }

    //close statement and connection
    //step 7: close connection, etc. 
    static void closeAll() {
        try {
            stmt.close();
            con.close();
        } catch (SQLException ex) {
            System.err.println("closeAll: " + ex.getMessage());
        }
    }
}

/*
Loads CSV file into flights set of tables in demo keyspace
Accepts csv file as a parameter
*/
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.*;
import com.datastax.driver.core.*;
 
public class LoadCSV2flights
{
  public static void main (String[] args)
  {
    //Get connected. Prepare tables space and tables if needed. 
    Cluster cluster;
    Session session;
    // Connect to the cluster and create keyspace "demo" if not exists after that connect
    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    session = cluster.connect("system");
    session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    session = cluster.connect("demo");
    session.execute("CREATE TABLE IF NOT EXISTS flights_load (ID int,YEAR int,DAY_OF_MONTH int,FL_DATE timestamp,AIRLINE_ID int,CARRIER varchar,FL_NUM int,ORIGIN_AIRPORT_ID int,ORIGIN varchar,ORIGIN_CITY_NAME varchar,ORIGIN_STATE_ABR varchar,DEST varchar,DEST_CITY_NAME varchar,DEST_STATE_ABR varchar,DEP_TIME varchar,ARR_TIME varchar,ACTUAL_ELAPSED_TIME varchar,AIR_TIME varchar,DISTANCE int, PRIMARY KEY(ID))");

    session.execute("CREATE TABLE IF NOT EXISTS flights_qa (ORIGIN varchar,FL_DATE timestamp,DEP_TIME varchar,ID int,YEAR int,DAY_OF_MONTH int,AIRLINE_ID int,CARRIER varchar,FL_NUM int,ORIGIN_AIRPORT_ID int,ORIGIN_CITY_NAME varchar,ORIGIN_STATE_ABR varchar,DEST varchar,DEST_CITY_NAME varchar,DEST_STATE_ABR varchar,ARR_TIME varchar,ACTUAL_ELAPSED_TIME varchar,AIR_TIME varchar,DISTANCE int, PRIMARY KEY((ORIGIN,YEAR),FL_DATE,DEP_TIME ,ID  ) ) WITH CLUSTERING ORDER BY (FL_DATE desc, DEP_TIME DESC, ID DESC)");

    session.execute("CREATE TABLE IF NOT EXISTS flights_buckets (CARRIER varchar,ORIGIN varchar,DEST varchar,AIR_TIME_BUCKETS int, ID int,FL_DATE timestamp,YEAR int,DAY_OF_MONTH int,AIRLINE_ID int,FL_NUM int,ORIGIN_AIRPORT_ID int,ORIGIN_CITY_NAME varchar,ORIGIN_STATE_ABR varchar,DEST_CITY_NAME varchar,DEST_STATE_ABR varchar,DEP_TIME varchar,ARR_TIME varchar,ACTUAL_ELAPSED_TIME varchar,AIR_TIME varchar,DISTANCE int, PRIMARY KEY((CARRIER,ORIGIN,DEST,YEAR),AIR_TIME_BUCKETS ,ID  ) ) WITH CLUSTERING ORDER BY (AIR_TIME_BUCKETS DESC, ID DESC)");


    PreparedStatement statement = session.prepare(
         "INSERT INTO flights_load" + "(id,year,day_of_month,fl_date,airline_id,carrier,fl_num,origin_airport_id,origin,origin_city_name,origin_state_abr,dest,"
+ "dest_city_name,dest_state_abr,dep_time,arr_time,actual_elapsed_time,air_time,distance)"
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
 
    BoundStatement boundStatement = new BoundStatement(statement);

    PreparedStatement statement_qa = session.prepare(
         "INSERT INTO flights_qa" + "(id,year,day_of_month,fl_date,airline_id,carrier,fl_num,origin_airport_id,origin,origin_city_name,origin_state_abr,dest,"
+ "dest_city_name,dest_state_abr,dep_time,arr_time,actual_elapsed_time,air_time,distance)"
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
 
    BoundStatement boundStatement_qa = new BoundStatement(statement_qa);

    PreparedStatement statement_buckets = session.prepare(
         "INSERT INTO flights_buckets" + "(id,year,day_of_month,fl_date,airline_id,carrier,fl_num,origin_airport_id,origin,origin_city_name,origin_state_abr,dest," + "dest_city_name,dest_state_abr,dep_time,arr_time,actual_elapsed_time,air_time,distance, air_time_buckets)"
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");
 
    BoundStatement boundStatement_buckets = new BoundStatement(statement_buckets);

    String fileToParse = args[0];
    BufferedReader fileReader = null;
    final String DELIMITER = ",";
	SimpleDateFormat inFormatter = new SimpleDateFormat("yyyy/MM/dd");
	SimpleDateFormat outFormatter = new SimpleDateFormat("yyyy-MM-dd");
    try
    {
      String line = "";
      fileReader = new BufferedReader(new FileReader(fileToParse));
      while ((line = fileReader.readLine()) != null)
      {
        String[] tokens = line.split(DELIMITER);
        //for(String token : tokens)
        //{
        //  System.out.println(token);
        //}
        session.execute(boundStatement.bind(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),inFormatter.parse(tokens[3]),Integer.parseInt(tokens[4]),tokens[5],Integer.parseInt(tokens[6]),Integer.parseInt(tokens[7]),tokens[8],tokens[9], tokens[10],tokens[11],tokens[12], tokens[13],tokens[14],tokens[15],tokens[16],tokens[17],Integer.parseInt(tokens[18])));

        session.execute(boundStatement_qa.bind(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),inFormatter.parse(tokens[3]),Integer.parseInt(tokens[4]),tokens[5],Integer.parseInt(tokens[6]),Integer.parseInt(tokens[7]),tokens[8],tokens[9], tokens[10],tokens[11],tokens[12], tokens[13],tokens[14],tokens[15],tokens[16],tokens[17],Integer.parseInt(tokens[18])));

        session.execute(boundStatement_buckets.bind(Integer.parseInt(tokens[0]),Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),inFormatter.parse(tokens[3]),Integer.parseInt(tokens[4]),tokens[5],Integer.parseInt(tokens[6]),Integer.parseInt(tokens[7]),tokens[8],tokens[9], tokens[10],tokens[11],tokens[12], tokens[13],tokens[14],tokens[15],tokens[16],tokens[17],Integer.parseInt(tokens[18]),Integer.parseInt(tokens[17]) / 10));
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      try
      {
       fileReader.close();
       cluster.close();
     }
      catch(IOException e)
      {
        e.printStackTrace();
      }
    }
  }
}

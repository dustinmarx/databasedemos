package dustin.examples.inparameters;

import static java.lang.System.out;

import oracle.jdbc.pool.OracleDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Demonstrate ORA-01795 in Oracle database and similar conditions in PostgreSQL.
 *
 * See also https://community.oracle.com/thread/235143.
 */
public class Main
{
   /**
    * First portion of SQL query that searches for numerals IN a range.
    */
   private final static String queryPrefix = "SELECT numeral1 FROM numeral WHERE numeral1 IN ";

   /**
    * Execute SQL query with prescribed number of IN values against Oracle database.
    */
   private void demonstrateOnOracleDatabase(final String sqlString)
   {
      Connection connection = null;
      try
      {
         final OracleDataSource ods = new OracleDataSource();
         ods.setURL("jdbc:oracle:thin:hr/hr@localhost:1521:XE");
         connection = ods.getConnection();
         final Statement statement = connection.createStatement();
         out.println("Oracle: " + sqlString);
         final ResultSet rs = statement.executeQuery(sqlString);
         if (rs.next())
         {
            out.println("\t" + rs.getInt(1));
         }
         rs.close();
      }
      catch (SQLException sqlException)
      {
         out.println("Unable to access Oracle Database - " + sqlException);
      }
      finally
      {
         if (connection != null)
         {
            try
            {
               connection.close();
            }
            catch (SQLException closeSqlException)
            {
               out.println("ERROR: Unable to close Oracle connection - " + closeSqlException);
            }
         }
      }
   }

   /**
    * Execute SQL query with prescribed number of IN values against PostgreSQL database.
    */
   private void demonstrateOnPostgreSqlDatabase(final String sqlString)
   {
      Connection connection = null;
      try
      {
         Class.forName("org.postgresql.Driver");
         connection = DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres");
         final Statement statement = connection.createStatement();
         out.println("PostgreSQL: " + sqlString);
         final ResultSet rs = statement.executeQuery(sqlString);
         if (rs.next())
         {
            out.println("\t" + rs.getInt(1));
         }
         rs.close();
      }
      catch (ClassNotFoundException cnfEx)
      {
         out.println("Unable to access PostgreSQL JDBC driver - " + cnfEx);
      }
      catch (SQLException sqlEx)
      {
         out.println("Unable to access PostgreSQL database - " + sqlEx);
      }
      finally
      {
         if (connection != null)
         {
            try
            {
               connection.close();
            }
            catch (SQLException connCloseEx)
            {
               out.println("ERROR: Unable to close PostgreSQL connection - " + connCloseEx);
            }
         }
      }
   }

   /**
    * Demonstrate running SELECT query statements using a WHERE clause with
    * IN and a prescribed number of values.
    *
    * @param arguments Command-line arguments: a single integer can be provided
    *    to indicate the number of values for the SQL SELECT query's WHERE ... IN.
    */
   public static void main(final String[] arguments)
   {
      int numberOfInValues = 1001;
      if (arguments.length > 0)
      {
         numberOfInValues = Integer.parseInt(arguments[0]);
      }

      final String inClauseTarget =
         IntStream.range(1, numberOfInValues+1).boxed().map(String::valueOf).collect(Collectors.joining(",", "(", ")"));
      final String select = queryPrefix + inClauseTarget;
      final Main instance = new Main();
      instance.demonstrateOnOracleDatabase(select);
      instance.demonstrateOnPostgreSqlDatabase(select);
   }
}

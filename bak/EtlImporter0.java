package edu.brown.cs.cs127.etl.importer;

import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import au.com.bytecode.opencsv.CSVReader;

public class EtlImporter {
	private static Connection _conn = null;

	/**
	 * You are only provided with a main method, but you may create as many new
	 * methods, other classes, etc as you want: just be sure that your
	 * application is runnable using the correct shell scripts.
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err
					.println("This application requires exactly four parameters: "
							+ "the path to the airports CSV, the path to the airlines CSV, "
							+ "the path to the flights CSV, and the full path where you would "
							+ "like the new SQLite database to be written to.");
			System.exit(1);
		}

		String AIRPORTS_FILE = args[0];
		String AIRLINES_FILE = args[1];
		String FLIGHTS_FILE = args[2];
		String DB_FILE = args[3];

		// Initialize the connection.
		Class.forName("org.sqlite.JDBC");
		_conn = DriverManager.getConnection("jdbc:sqlite:data.db");

		System.out.println("Connect to db successfully.");

		Statement stmt = _conn.createStatement();

		// ENABLE FOREIGN KEY CONSTRAINT CHECKING
		stmt.executeUpdate("PRAGMA foreign_keys = ON;");
		// Speed up INSERTs
		stmt.executeUpdate("PRAGMA synchronous = OFF;");
		stmt.executeUpdate("PRAGMA journal_mode = MEMORY;");
		
		stmt.executeUpdate("drop table if exists DelaySource");
		stmt.executeUpdate("drop table if exists CanceledFlight");		
		stmt.executeUpdate("drop table if exists FlightRecord");	
		stmt.executeUpdate("drop table if exists FlightSchedule");		
		stmt.executeUpdate("drop table if exists Airport");
		stmt.executeUpdate("drop table if exists Airline");


		String sMakeAirport = "CREATE TABLE Airport ( " + "Code	varchar(5), "
				+ "Name	varchar(100) not null, " + "City	varchar(20), "
				+ "State	varchar(15), " + "primary key(Code)); ";
		stmt.executeUpdate(sMakeAirport);

		String sMakeAirline = "CREATE TABLE Airline ( " + "Code	varchar(8), "
				+ "Name	varchar(60) not null, " + "primary key(Code)); ";
		stmt.executeUpdate(sMakeAirline);

		String sMakeFlightSchedule = "CREATE TABLE FlightSchedule( "
				+ "ID int, " + "AirlineCode varchar(8) not null, "
				+ "FlightNum varchar(8) not null, "
				+ "sDeptTime text not null, " + "sArrTime text not null, "
				+ "sArrDay int not null, "
				+ "DeptAirportCode varchar(5) not null, "
				+ "ArrAirportCode varchar(5) not null, " + "primary key(ID), "
				+ "foreign key(AirlineCode) references Airline, "
				+ "foreign key(DeptAirportCode) references Airport, "
				+ "foreign key(ArrAirportCode) references Airport, "
				+ "check( time(sDeptTime) < time(sArrTime)  or sArrDay > 0) "
				+ "); ";
		stmt.executeUpdate(sMakeFlightSchedule);

		String sMakeCanceledFlight = "CREATE TABLE CanceledFlight ( "
				+ "ID int, " + "sDeptDate text not null, "
				+ "primary key(ID, sDeptDate), "
				+ "foreign key(ID) references FlightSchedule " + "); ";
		stmt.executeUpdate(sMakeCanceledFlight);

		String sMakeFlightRecord = "CREATE TABLE FlightRecord ( "
				+ "ID int, " + "sDeptDate text not null,  "
				+ "DeptDelay int not null,   " + "ArrDelay int not null, "
				+ "primary key(ID, sDeptDate), "
				+ "foreign key(ID) references FlightSchedule " + "); ";
		stmt.executeUpdate(sMakeFlightRecord);

		String sMakeDelaySource = "CREATE TABLE DelaySource ( "
				+ "ID int, "
				+ "sDeptDate text not null, "
				+ "Name text check( Name in ('CarrierDelay',  'WeatherDelay',  'AirTrafficDelay',  'SecurityDelay')), "
				+ "DelayMinutes int not null, "
				+ "primary key(ID, sDeptDate, Name), "
				+ "foreign key(ID, sDeptDate) references FlightRecord, " 
				+ "check( DelayMinutes >= 0) )";
		stmt.executeUpdate(sMakeDelaySource);

		// Airport file data import
		_conn.setAutoCommit(false);
		CSVReader reader = new CSVReader(new FileReader(AIRPORTS_FILE));
		String[] nextLine;
		PreparedStatement prep = _conn
				.prepareStatement("INSERT OR IGNORE INTO Airport (Code, Name, City, State) VALUES (?, ?, ?, ?)");
		while ((nextLine = reader.readNext()) != null) {
			if (nextLine.length != 2) {
				System.out.println("ERROR: wrong file format " + AIRPORTS_FILE);
				continue;
			}
			String code = nextLine[0].replaceAll("^\"|\"$", "");
			
			String name = nextLine[1].replaceAll("^\"|\"$", "");
			
			prep.setString(1, code);
			prep.setString(2, name);
			prep.setString(3, "");
			prep.setString(4, "");
			prep.addBatch();
		}
		prep.executeBatch();
		_conn.setAutoCommit(true);

		// Airline file data import
		_conn.setAutoCommit(false);
		reader = new CSVReader(new FileReader(AIRLINES_FILE));
		prep = _conn
				.prepareStatement("INSERT OR IGNORE INTO Airline (Code, Name) VALUES (?, ?)");
		while ((nextLine = reader.readNext()) != null) {
			if (nextLine.length != 2) {
				System.out.println("ERROR: wrong file format " + AIRLINES_FILE);
				continue;
			}
			String code = nextLine[0].replaceAll("^\"|\"$", "");
			
			String name = nextLine[1].replaceAll("^\"|\"$", "");
			

			prep.setString(1, code);
			prep.setString(2, name);
			prep.addBatch();
		}
		prep.executeBatch();
		_conn.setAutoCommit(true);

		// import data in flights.csv
		// Airline file data import
		int assignedFlightID = 0;
		int lineCount = 0;
		//_conn.setAutoCommit(false);
		reader = new CSVReader(new FileReader(FLIGHTS_FILE));
		while ((nextLine = reader.readNext()) != null) {
			lineCount++;
			System.out.println("Processing line " + lineCount);
			if (nextLine.length != 19) {
				System.out.println("ERROR: wrong flightfile format. Number of text fields of this line is no 19");
				continue;
			}

			String airlineCode;
			String flightNum;
			String deptAirportCode;
			String deptAirportCity;
			String deptAirportState;
			String arrAirportCode;
			String arrAirportCity;
			String arrAirportState;
			String sDeptDate;
			String sDeptTime;
			Integer deptDelay;
			String sArrDate;
			String sArrTime;
			Integer arrDelay;
			Integer canceled;
			Integer carrierDelay;
			Integer weatherDelay;
			Integer airControlDelay;
			Integer securityDelay;

			try {
				airlineCode = nextLine[0].replaceAll("^\"|\"$", "");
				flightNum = nextLine[1].replaceAll("^\"|\"$", "");
				deptAirportCode = nextLine[2].replaceAll("^\"|\"$", "");
				deptAirportCity = nextLine[3].replaceAll("^\"|\"$", "");
				deptAirportState = nextLine[4].replaceAll("^\"|\"$", "");
				arrAirportCode = nextLine[5].replaceAll("^\"|\"$", "");
				arrAirportCity = nextLine[6].replaceAll("^\"|\"$", "");
				arrAirportState = nextLine[7].replaceAll("^\"|\"$", "");
				sDeptDate = nextLine[8].replaceAll("^\"|\"$", "");
				sDeptTime = nextLine[9].replaceAll("^\"|\"$", "");
				deptDelay = Integer.parseInt(nextLine[10]);
				sArrDate = nextLine[11].replaceAll("^\"|\"$", "");
				sArrTime = nextLine[12].replaceAll("^\"|\"$", "");
				arrDelay = Integer.parseInt(nextLine[13]);
				canceled = Integer.parseInt(nextLine[14]);
				carrierDelay = Integer.parseInt(nextLine[15]);
				weatherDelay = Integer.parseInt(nextLine[16]);
				airControlDelay = Integer.parseInt(nextLine[17]);
				securityDelay = Integer.parseInt(nextLine[18]);

			} catch (NumberFormatException e) {
				System.out.println("ERROR: wrong flightfile format: stringTonumber format exception" );
				e.printStackTrace(System.out);
				continue;
			}

			boolean isValid = false;
			try {
				sDeptTime = standardlizeTime(sDeptTime);
				sArrTime = standardlizeTime(sArrTime);
				sDeptDate = standardlizeDate(sDeptDate);
				sArrDate = standardlizeDate(sArrDate);
				isValid = VerifyValidity(airlineCode, deptAirportCode,
						arrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime,
						deptDelay, arrDelay);
				if(!isValid) {
					System.out.println("Invalid line");
					continue;
				}			
			} catch(ParseException e)  {
				System.out.println("ERROR: wrong flightfile format: parseException");		
				e.printStackTrace(System.out);
				continue;
			}
			
			int sArrDay = scheduleDeptArrDateDiff(sDeptDate, sArrDate);
			
			int flightID = ifFlightScheduleExist(airlineCode, flightNum, sDeptTime, sArrTime, 
					sArrDay, deptAirportCode, arrAirportCode);
			
			if(flightID == 0) {
				assignedFlightID++;
				
				prep = _conn
						.prepareStatement("INSERT INTO FlightSchedule (ID, AirlineCode, FlightNum, sDeptTime, sArrTime, sArrDay, DeptAirportCode, ArrAirportCode) " +
				         " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");				
				
				prep.setInt(1, assignedFlightID);
				prep.setString(2, airlineCode);
				prep.setString(3, flightNum);
				prep.setString(4, sDeptTime);
				prep.setString(5, sArrTime);
				prep.setInt(6, sArrDay);
				prep.setString(7, deptAirportCode);
				prep.setString(8, arrAirportCode);		
				prep.executeUpdate();
				flightID = assignedFlightID;
				
				//insertOrUpdateAirportInfo(deptAirportCode, deptAirportCity, deptAirportState);		
				//insertOrUpdateAirportInfo(arrAirportCode, arrAirportCity, arrAirportState);
				
			} 			
			
			if(canceled > 0) {
				prep = _conn
						.prepareStatement("INSERT INTO CanceledFlight (ID, sDeptDate) " +
								" VALUES (?, ?)");
				prep.setInt(1, flightID);
				prep.setString(2,  sDeptDate);
				prep.executeUpdate();
				
			} else {
				prep = _conn
						.prepareStatement("INSERT INTO FlightRecord (ID, sDeptDate, DeptDelay, ArrDelay) " +
								" VALUES (?, ?, ?, ?)");
				prep.setInt(1, flightID);
				prep.setString(2,  sDeptDate);
				prep.setInt(3,  deptDelay);
				prep.setInt(4,  arrDelay);
				prep.executeUpdate();
				
				prep = _conn
						.prepareStatement("INSERT INTO DelaySource (ID, sDeptDate, Name, DelayMinutes) " +
								" VALUES (?, ?, ?, ?)");
				prep.setInt(1, flightID);
				prep.setString(2,  sDeptDate);				
				prep.setString(3,  "CarrierDelay");
				prep.setInt(4,  carrierDelay);				
				prep.addBatch();

				
				prep.setString(3,  "WeatherDelay");
				prep.setInt(4,  weatherDelay);				
				prep.addBatch();
				
				prep.setString(3,  "AirTrafficDelay");
				prep.setInt(4,  airControlDelay);				
				prep.addBatch();
				
				prep.setString(3,  "SecurityDelay");
				prep.setInt(4,  securityDelay);				
				prep.addBatch();				
				
				prep.executeBatch();				
			}
		}
		
		
		/*
		 * READING DATA FROM CSV FILES Source:
		 * http://opencsv.sourceforge.net/#how-to-read
		 * 
		 * If you want to use an Iterator style pattern, you might do something
		 * like this:
		 * 
		 * CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
		 * String [] nextLine; while ((nextLine = reader.readNext()) != null) {
		 * // nextLine[] is an array of values from the line
		 * System.out.println(nextLine[0] + nextLine[1] + "etc..."); }
		 * 
		 * Or, if you might just want to slurp the whole lot into a List, just
		 * call readAll()...
		 * 
		 * CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
		 * List myEntries = reader.readAll();
		 */

		/*
		 * Below are some snippets of JDBC code that may prove useful
		 * 
		 * For more sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321
		 * /http://www.zentus.com/sqlitejdbc/
		 * 
		 * ---
		 * 
		 * // INITIALIZE THE CONNECTION Class.forName("org.sqlite.JDBC");
		 * Connection conn = DriverManager.getConnection("jdbc:sqlite:" +
		 * DB_FILE);
		 * 
		 * ---
		 * 
		 * // ENABLE FOREIGN KEY CONSTRAINT CHECKING Statement stat =
		 * conn.createStatement();
		 * stat.executeUpdate("PRAGMA foreign_keys = ON;");
		 * 
		 * // Speed up INSERTs stat.executeUpdate("PRAGMA synchronous = OFF;");
		 * stat.executeUpdate("PRAGMA journal_mode = MEMORY;");
		 * 
		 * ---
		 * 
		 * // You can execute DELETE statements before importing if you want to
		 * be // able to overwrite an existing database.
		 * stat.executeUpdate("DROP TABLE IF EXISTS table;");
		 * 
		 * ---
		 * 
		 * // Normally the database throws an exception when constraints are
		 * enforced // and an INSERT statement that violates a constraint is
		 * executed. This is true // even when doing a batch insert (multiple
		 * rows in one statement), causing all // rows in the statement to not
		 * be inserted into the database.
		 * 
		 * // As a result, if you want the efficiency gains of using batch
		 * inserts, you need to be smart: // You need to make sure your
		 * application enforces foreign key constraints before the insert ever
		 * happens. PreparedStatement prep = conn.prepareStatement(
		 * "INSERT OR IGNORE INTO table (col1, col2) VALUES (?, ?)");
		 * List<String[]> rowInfo = getTableRows(); for (String[] curRow :
		 * rowInfo) { prep.setString(1, curRow[0]); prep.setInt(2, curRow[1]);
		 * prep.addBatch(); }
		 * 
		 * // We temporarily disable auto-commit, allowing the batch to be sent
		 * // as one single transaction. Then we re-enable it, executing the
		 * batch. conn.setAutoCommit(false); prep.executeBatch();
		 * conn.setAutoCommit(true);
		 */
	}

	static void insertOrUpdateAirportInfo(String airportCode, String airportCity, String airportState) throws SQLException {
		PreparedStatement prep = _conn
				.prepareStatement("select Count(*) as count from Airport where Code = ? and City = ? and State = ? ");
		
		prep.setString(1,  airportCode);				
		prep.setString(2,  airportCity);
		prep.setString(3,  airportState);
		
		ResultSet rs = prep.executeQuery();
		int count = 0;
		while (rs.next()) {
			count = rs.getInt("count");
		}

		if (count == 0) {
			prep = _conn
					.prepareStatement("update Airport set City = ? where Code = ?; ");
			prep.setString(1,  airportCity);				
			prep.setString(2,  airportCode);
			prep.executeUpdate();
			
			prep = _conn
					.prepareStatement("update Airport set State = ? where Code = ?; ");
			prep.setString(1,  airportState);				
			prep.setString(2,  airportCode);
			prep.executeUpdate();			
		}
		
		return;		
	}
	
	static int scheduleDeptArrDateDiff(String sDeptDate, String sArrDate) throws ParseException  {
		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

		Date sDeptDateT = DATE_FORMAT.parse(sDeptDate);
		Date sArrDateT = DATE_FORMAT.parse(sArrDate);		
		int diffInDays = (int)( (sArrDateT.getTime() - sDeptDateT.getTime()) / (1000 * 60 * 60 * 24) );	
		return diffInDays;
	}
	
	static int ifFlightScheduleExist(String airlineCode, String flightNum, String sDeptTime, String sArrTime, 
			int sArrDay, String deptAirportCode, String arrAirportCode)	throws SQLException {
		
		PreparedStatement pStmt = null;

		String sql = "SELECT ID FROM FlightSchedule "
				+ "WHERE AirlineCode = ? and FlightNum = ? and sDeptTime = ? and "  
				+ " sArrTime = ? and sArrDay = ? and DeptAirportCode = ? and ArrAirportCode = ? ;";

		pStmt = _conn.prepareStatement(sql);
		pStmt.setString(1, airlineCode); 
		pStmt.setString(2, flightNum); 
		pStmt.setString(3, sDeptTime); 
		pStmt.setString(4, sArrTime); 
		pStmt.setInt(5, sArrDay); 
		pStmt.setString(6, deptAirportCode); 
		pStmt.setString(7, arrAirportCode); 	

		ResultSet rs = pStmt.executeQuery();
		int ID = 0;   // 0 means not exist
		while (rs.next()) {
			ID = rs.getInt("ID");
		}

		return ID;
	}
	
	static boolean verifyAirlineCodeValid(String airlineCode)
			throws SQLException {
		PreparedStatement pStmt = null;

		String sql = "SELECT count(*) as count " + "FROM Airline "
				+ "WHERE Code = ? ;";

		pStmt = _conn.prepareStatement(sql);
		pStmt.setString(1, airlineCode);

		ResultSet rs = pStmt.executeQuery();
		int count = 0;
		while (rs.next()) {
			count = rs.getInt("count");
		}

		if (count == 0) {
			System.out.println("Airline Code is invalod:" + airlineCode);
			return false;
		} else {
			return true;
		}
	}

	static boolean verifyAirportCodeValid(String airportCode)
			throws SQLException {
		PreparedStatement pStmt = null;

		String sql = "SELECT count(*) as count " + "FROM Airport "
				+ "WHERE Code = ? ;";

		pStmt = _conn.prepareStatement(sql);
		pStmt.setString(1, airportCode);

		ResultSet rs = pStmt.executeQuery();
		int count = 0;
		while (rs.next()) {
			count = rs.getInt("count");
		}

		if (count == 0) {
			System.out.println("Airport Code is invalod:" + airportCode);
			return false;
		} else {
			return true;
		}
	}

	static boolean VerifyValidity(String airlineCode, String deptAirportCode,
			String arrAirportCode, String sDeptDate, String sDeptTime,
			String sArrDate, String sArrTime, int deptDelay, int arrDelay)
			throws SQLException, ParseException {

		boolean tmp = verifyAirlineCodeValid(airlineCode)
				&& verifyAirportCodeValid(deptAirportCode)
				&& verifyAirportCodeValid(arrAirportCode);

		if (tmp == false) {
			System.out.println("Airline Code or AirprtCode invalid.");
			return false;
		}
		
		String sDeptDateTime = sDeptDate + " " + sDeptTime;
		String sArrDateTime = sArrDate + " " + sArrTime;

		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		Date sDeptDateT = DATE_FORMAT.parse(sDeptDateTime);
		Date sArrDateT = DATE_FORMAT.parse(sArrDateTime);
		if (sDeptDateT.after(sArrDateT)) {
			System.out.println("scheduled departure time is after scheduled arrival time.");
			return false;
		}

		Calendar cal = Calendar.getInstance();
		//cal.setTimeZone("");
		cal.setTime(sDeptDateT);  
		cal.add(Calendar.MINUTE, deptDelay);
		Date aDeptDateT = cal.getTime();    //actual departure time and date

		cal.setTime(sArrDateT);
		cal.add(Calendar.MINUTE, arrDelay);
		Date aArrDateT = cal.getTime();     //actual arrival time and date
		
		if (aDeptDateT.after(aArrDateT)) {
			System.out.println("actual departure time is after actual arrival time.");
			return false;
		}

		return true;
	}

	static String standardlizeTime(String inputTime) throws ParseException {
		SimpleDateFormat[] testInputFormat = new SimpleDateFormat[3];

		SimpleDateFormat returnFormat = new SimpleDateFormat("HH:mm");
		testInputFormat[0] = new SimpleDateFormat("hh:mm a");
		testInputFormat[1] = new SimpleDateFormat("KK:mm a");
		testInputFormat[2] = new SimpleDateFormat("HH:mm");
		
		testInputFormat[0].setLenient(false);
		testInputFormat[1].setLenient(false);
		testInputFormat[2].setLenient(false);		

		Date time;
		for (int i = 0; i < testInputFormat.length; i++) {
			try {
				time = testInputFormat[i].parse(inputTime);
				return returnFormat.format(time);
			} catch (ParseException e) {
				if (i == testInputFormat.length - 1) {
					throw e;
				}
			}
		}

		return "";
	}

	static String standardlizeDate(String inputDate) throws ParseException {
		SimpleDateFormat[] testInputFormat = new SimpleDateFormat[4];

		SimpleDateFormat returnFormat = new SimpleDateFormat("yyyy-MM-dd");
		testInputFormat[0] = new SimpleDateFormat("yyyy/MM/dd");
		testInputFormat[1] = new SimpleDateFormat("yyyy-MM-dd");
		testInputFormat[2] = new SimpleDateFormat("MM/dd/yyyy");
		testInputFormat[3] = new SimpleDateFormat("MM-dd-yyyy");
		
		testInputFormat[0].setLenient(false);
		testInputFormat[1].setLenient(false);
		testInputFormat[2].setLenient(false);
		testInputFormat[3].setLenient(false);		
		Date date;
		for (int i = 0; i < testInputFormat.length; i++) {
			try {
				date = testInputFormat[i].parse(inputDate);
				return returnFormat.format(date);
			} catch (ParseException e) {
				if (i == testInputFormat.length - 1) {
					throw e;
				}
			}
		}

		return "";
	}
}

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
import java.util.Iterator;
import java.util.List;

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
		_conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);

		System.out.println("Connect to db successfully.");

		Statement stmt = _conn.createStatement();

		// ENABLE FOREIGN KEY CONSTRAINT CHECKING
		stmt.executeUpdate("PRAGMA foreign_keys = ON;");
		// Speed up INSERTs
		stmt.executeUpdate("PRAGMA synchronous = OFF;");
		stmt.executeUpdate("PRAGMA journal_mode = MEMORY;");
		
		stmt.executeUpdate("drop table if exists DelayReason");
		stmt.executeUpdate("drop table if exists CanceledFlight");		
		stmt.executeUpdate("drop table if exists DelayedFlight");	
		stmt.executeUpdate("drop table if exists Flight");		
		stmt.executeUpdate("drop table if exists Airport");
		stmt.executeUpdate("drop table if exists Airline");
		stmt.executeUpdate("drop table if exists tmpFlight");		


		String sMakeAirport = "CREATE TABLE Airport ( " + "Code	varchar(5), "
				+ "Name	varchar(100) not null, " + "City	varchar(20), "
				+ "State	varchar(15), " + "primary key(Code)); ";
		stmt.executeUpdate(sMakeAirport);

		String sMakeAirline = "CREATE TABLE Airline ( " + "Code	varchar(8), "
				+ "Name	varchar(60) not null, " + "primary key(Code)); ";
		stmt.executeUpdate(sMakeAirline);
		
		String sMakeFlight = "CREATE TABLE Flight( "
				+ "ID int, " 
				+ "AirlineCode varchar(8) not null, "
				+ "FlightNum varchar(8) not null, "
				+ "DeptAirportCode varchar(5) not null, "
				+ "ArrAirportCode varchar(5) not null, " 				
				+ "sDeptDate text not null, "
				+ "sDeptTime text not null, " 
				+ "sArrDate text not null, "
				+ "sArrTime text not null, "
				+ "primary key(ID), "
				+ "foreign key(AirlineCode) references Airline, "
				+ "foreign key(DeptAirportCode) references Airport, "
				+ "foreign key(ArrAirportCode) references Airport, "
				+ "check ( julianday (sDeptDate || ' ' || sDeptTime ) < julianday (sArrDate || ' ' || sArrTime ) ) "
				//+ "check( (time(sDeptTime) < time(sArrTime) and time(sArrDate) = time(sDeptDate) ) or ( time(sArrDate) > time(sDeptDate) )) "
				+ "); ";
		stmt.executeUpdate(sMakeFlight);
		
		String sMakeCanceledFlight = "CREATE TABLE CanceledFlight ( "
				+ "ID int, " 
				+ "primary key(ID), "
				+ "foreign key(ID) references Flight "
				+ "); ";
		stmt.executeUpdate(sMakeCanceledFlight);
		
		String sMakeDelayedFlight = "CREATE TABLE DelayedFlight ( "
				+ "ID int, " 
				+ "DeptDelay int not null, " 
				+ "ArrDelay int not null, "
				+ "primary key(ID), "
				+ "foreign key(ID) references Flight " 
				+ "); ";
		stmt.executeUpdate(sMakeDelayedFlight);
		
		String sMakeDelayReason = "CREATE TABLE DelayReason ( "
				+ "ID int, "
				+ "Reason text check( Reason in ('CarrierDelay',  'WeatherDelay',  'AirTrafficDelay',  'SecurityDelay')), "
				+ "DelayMinutes int not null, "
				+ "primary key(ID, Reason), "
				+ "foreign key(ID) references DelayedFlight, " 
				+ "check( DelayMinutes > 0) )";
		stmt.executeUpdate(sMakeDelayReason);
		
		// Airport file data import
		_conn.setAutoCommit(false);
		CSVReader reader = new CSVReader(new FileReader(AIRPORTS_FILE));
		String[] nextLine;
		PreparedStatement prep = _conn.prepareStatement("INSERT OR IGNORE INTO Airport (Code, Name, City, State) VALUES (?, ?, ?, ?)");
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

		
		// import the flight file into a tmp table 
		_conn.setAutoCommit(false);
		String sMakeTmpFlight = "CREATE temp TABLE tmpFlight ( " +
				"AirlineCode varchar(5), " +
				"FlightNum varchar(8), " +
				"DeptAirportCode varchar(5), " +
				"DeptAirportCity varchar(20), "+
				"DeptAirportState varchar(15), " +
				"ArrAirportCode varchar(5), " +
				"ArrAirportCity varchar(20), "+
				"ArrAirportState varchar(15), "+
				"sDeptDate  text, " +
				"sDeptTime text, "+
				"DeptDelay int, " +
				"sArrDate text, " +
				"sArrTime text, " +
				"ArrDelay int, " +
				"Canceled int, " +
				"CarrierDelay int, " +
				"WeatherDelay int, " +
				"AirTrafficDelay int, "+
				"securityDelay int); ";				

		stmt.executeUpdate(sMakeTmpFlight);
		
		prep = _conn
				.prepareStatement("INSERT OR IGNORE INTO tmpFlight VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
		int lineCount = 0;

		reader = new CSVReader(new FileReader(FLIGHTS_FILE));
		List<String[]> myDatas = reader.readAll();
		
		for (String[] nextRow : myDatas) {
			nextLine = nextRow;
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
				
				if(carrierDelay < 0 || weatherDelay < 0 || airControlDelay < 0 || securityDelay < 0) {
					System.out.println("ERROR: wrong flightfile format: delay number is negative.");
					continue;
				}
				
				try {
					sDeptTime = standardlizeTime(sDeptTime);
					sArrTime = standardlizeTime(sArrTime);
					sDeptDate = standardlizeDate(sDeptDate);
					sArrDate = standardlizeDate(sArrDate);
					boolean isValid = VerifyValidity2(sDeptDate, sDeptTime, sArrDate, sArrTime,
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
				
				prep.setString(1, airlineCode);
				prep.setString(2, flightNum);
				prep.setString(3, deptAirportCode);
				prep.setString(4, deptAirportCity);
				prep.setString(5, deptAirportState);
				prep.setString(6, arrAirportCode);
				prep.setString(7, arrAirportCity);
				prep.setString(8, arrAirportState);
				prep.setString(9, sDeptDate);
				prep.setString(10, sDeptTime);
				prep.setInt(11, deptDelay);
				prep.setString(12, sArrDate);	
				prep.setString(13, sArrTime);
				prep.setInt(14, arrDelay);
				prep.setInt(15, canceled);
				prep.setInt(16, carrierDelay);	
				prep.setInt(17, weatherDelay);
				prep.setInt(18, airControlDelay);
				prep.setInt(19, securityDelay);
				
				prep.addBatch();
				
			} catch (NumberFormatException e) {
				System.out.println("ERROR: wrong flightfile format: stringTonumber format exception" );
				e.printStackTrace(System.out);
				continue;
			}			
		}
		prep.executeBatch();
		_conn.setAutoCommit(true);	
		
		stmt.executeUpdate("delete from tmpFlight where AirlineCode not in (select Code from Airline); ");
		stmt.executeUpdate("delete from tmpFlight where ArrAirportCode not in (select Code from Airport); ");
		stmt.executeUpdate("delete from tmpFlight where DeptAirportCode not in (select Code from Airport); ");			

		stmt.executeUpdate("create temp table tmp1 as  SELECT DISTINCT DeptAirportCode, DeptAirportCity, DeptAirportState from tmpFlight;");
		stmt.executeUpdate("update Airport set City = (select tmp1.DeptAirportCity from tmp1 where Airport.Code = tmp1.DeptAirportCode);");
		stmt.executeUpdate("update Airport set State = (select  tmp1.DeptAirportState from tmp1 where Airport.Code = tmp1.DeptAirportCode);");

		stmt.executeUpdate("create temp table tmp2 as  SELECT DISTINCT ArrAirportCode, ArrAirportCity, ArrAirportState from tmpFlight;");
		stmt.executeUpdate("update Airport set City = (select tmp2.ArrAirportCity from tmp2 where Airport.Code = tmp2.ArrAirportCode);");
		stmt.executeUpdate("update Airport set State = (select  tmp2.ArrAirportState from tmp2 where Airport.Code = tmp2.ArrAirportCode);");		
		stmt.executeUpdate("drop table if exists tmp1");	
		stmt.executeUpdate("drop table if exists tmp2");
		
		System.out.print("Begin to create Flight table....");
		stmt.executeUpdate("create table tmpFlight2 as SELECT AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime FROM tmpFlight;");
		stmt.executeUpdate("ALTER TABLE tmpFlight2 ADD COLUMN ID int;");
		stmt.executeUpdate("update tmpFlight2 set ID = rowid;");
		stmt.executeUpdate("insert into Flight select ID, AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from tmpFlight2;");
		stmt.executeUpdate("drop table if exists tmpFlight2");
		System.out.println("done");
		
		System.out.print("Begin to create CanceledFlight table....");
		stmt.executeUpdate("insert into CanceledFlight " + 
							"select Flight.ID " +
							"from Flight join tmpFlight " +
							"on Flight.AirlineCode = tmpFlight.AirlineCode and " +
							"Flight.FlightNum = tmpFlight.FlightNum and " +
							"Flight.DeptAirportCode = tmpFlight.DeptAirportCode and " +
							"Flight.ArrAirportCode = tmpFlight.ArrAirportCode and " +
							"Flight.sDeptDate = tmpFlight.sDeptDate and " +
							"Flight.sDeptTime = tmpFlight.sDeptTime and " +
							"Flight.sArrDate = tmpFlight.sArrDate and " +
							"Flight.sArrTime = tmpFlight.sArrTime " +
							"where tmpFlight.Canceled = 1 ");
		System.out.println("done");
		
		System.out.print("Begin to create DelayedFlight table....");
		stmt.executeUpdate("insert into DelayedFlight " + 
				"select Flight.ID, tmpFlight.DeptDelay, tmpFlight.ArrDelay " +
				"from Flight join tmpFlight " +
				"on Flight.AirlineCode = tmpFlight.AirlineCode and " +
				"Flight.FlightNum = tmpFlight.FlightNum and " +
				"Flight.DeptAirportCode = tmpFlight.DeptAirportCode and " +
				"Flight.ArrAirportCode = tmpFlight.ArrAirportCode and " +
				"Flight.sDeptDate = tmpFlight.sDeptDate and " +
				"Flight.sDeptTime = tmpFlight.sDeptTime and " +
				"Flight.sArrDate = tmpFlight.sArrDate and " +
				"Flight.sArrTime = tmpFlight.sArrTime " +
				"where tmpFlight.Canceled = 0 ");
		System.out.println("done");
		
		System.out.print("Begin to create DelayReason table....");
		stmt.executeUpdate("create table tmpDelayReason1 as " +
							"select Flight.ID, tmpFlight.CarrierDelay " +
							"from Flight join tmpFlight " +
							"on Flight.AirlineCode = tmpFlight.AirlineCode and   " +
							"Flight.FlightNum = tmpFlight.FlightNum and " +
							"Flight.DeptAirportCode = tmpFlight.DeptAirportCode and " +
							"Flight.sDeptDate = tmpFlight.sDeptDate and " +
							"Flight.sDeptTime = tmpFlight.sDeptTime and " +
							"Flight.sArrDate = tmpFlight.sArrDate and " +
							"Flight.sArrTime = tmpFlight.sArrTime " +
							"where tmpFlight.Canceled = 0 and tmpFlight.CarrierDelay > 0 ");		
		stmt.executeUpdate("ALTER TABLE tmpDelayReason1 ADD COLUMN Reason text;");
		stmt.executeUpdate("update tmpDelayReason1 set Reason = 'CarrierDelay';");
		stmt.executeUpdate("insert into DelayReason select ID, Reason, CarrierDelay as DelayMinutes from tmpDelayReason1;");
		stmt.executeUpdate("drop table if exists tmpDelayReason1;");
		 
		stmt.executeUpdate("create table tmpDelayReason2 as " +
				"select Flight.ID, tmpFlight.WeatherDelay " +
				"from Flight join tmpFlight " +
				"on Flight.AirlineCode = tmpFlight.AirlineCode and   " +
				"Flight.FlightNum = tmpFlight.FlightNum and " +
				"Flight.DeptAirportCode = tmpFlight.DeptAirportCode and " +
				"Flight.sDeptDate = tmpFlight.sDeptDate and " +
				"Flight.sDeptTime = tmpFlight.sDeptTime and " +
				"Flight.sArrDate = tmpFlight.sArrDate and " +
				"Flight.sArrTime = tmpFlight.sArrTime " +
				"where tmpFlight.Canceled = 0 and tmpFlight.WeatherDelay > 0 ");		
		stmt.executeUpdate("ALTER TABLE tmpDelayReason2 ADD COLUMN Reason text;");
		stmt.executeUpdate("update tmpDelayReason2 set Reason = 'WeatherDelay';");
		stmt.executeUpdate("insert into DelayReason select ID, Reason, WeatherDelay as DelayMinutes from tmpDelayReason2;");
		stmt.executeUpdate("drop table if exists tmpDelayReason2;");	
	
		stmt.executeUpdate("create table tmpDelayReason3 as " +
				"select Flight.ID, tmpFlight.AirTrafficDelay " +
				"from Flight join tmpFlight " +
				"on Flight.AirlineCode = tmpFlight.AirlineCode and   " +
				"Flight.FlightNum = tmpFlight.FlightNum and " +
				"Flight.DeptAirportCode = tmpFlight.DeptAirportCode and " +
				"Flight.sDeptDate = tmpFlight.sDeptDate and " +
				"Flight.sDeptTime = tmpFlight.sDeptTime and " +
				"Flight.sArrDate = tmpFlight.sArrDate and " +
				"Flight.sArrTime = tmpFlight.sArrTime " +
				"where tmpFlight.Canceled = 0 and tmpFlight.AirTrafficDelay > 0 ");		
		stmt.executeUpdate("ALTER TABLE tmpDelayReason3 ADD COLUMN Reason text;");
		stmt.executeUpdate("update tmpDelayReason3 set Reason = 'AirTrafficDelay';");
		stmt.executeUpdate("insert into DelayReason select ID, Reason, AirTrafficDelay as DelayMinutes from tmpDelayReason3;");
		stmt.executeUpdate("drop table if exists tmpDelayReason3;");

		stmt.executeUpdate("create table tmpDelayReason4 as " +
				"select Flight.ID, tmpFlight.securityDelay " +
				"from Flight join tmpFlight " +
				"on Flight.AirlineCode = tmpFlight.AirlineCode and   " +
				"Flight.FlightNum = tmpFlight.FlightNum and " +
				"Flight.DeptAirportCode = tmpFlight.DeptAirportCode and " +
				"Flight.sDeptDate = tmpFlight.sDeptDate and " +
				"Flight.sDeptTime = tmpFlight.sDeptTime and " +
				"Flight.sArrDate = tmpFlight.sArrDate and " +
				"Flight.sArrTime = tmpFlight.sArrTime " +
				"where tmpFlight.Canceled = 0 and tmpFlight.securityDelay > 0 ");		
		stmt.executeUpdate("ALTER TABLE tmpDelayReason4 ADD COLUMN Reason text;");
		stmt.executeUpdate("update tmpDelayReason4 set Reason = 'SecurityDelay';");
		stmt.executeUpdate("insert into DelayReason select ID, Reason, securityDelay as DelayMinutes from tmpDelayReason4;");
		stmt.executeUpdate("drop table if exists tmpDelayReason4;");
		
		System.out.println("done");
		stmt.executeUpdate("drop table if exists tmpFlight");	
		
		System.out.println("Import Finsihed!");		
	}


	static boolean VerifyValidity2(String sDeptDate, String sDeptTime,
			String sArrDate, String sArrTime, int deptDelay, int arrDelay)
			throws SQLException, ParseException {
	
		String sDeptDateTime = sDeptDate + " " + sDeptTime;
		String sArrDateTime = sArrDate + " " + sArrTime;

		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");

		Date sDeptDateT = DATE_FORMAT.parse(sDeptDateTime);
		Date sArrDateT = DATE_FORMAT.parse(sArrDateTime);
		if (sDeptDateT.after(sArrDateT)) {
			System.out.println("scheduled departure time is after scheduled arrival time.");
			return false;
		}

//		Calendar cal = Calendar.getInstance();
//		//cal.setTimeZone("");
//		cal.setTime(sDeptDateT);  
//		cal.add(Calendar.MINUTE, deptDelay);
//		Date aDeptDateT = cal.getTime();    //actual departure time and date
//
//		cal.setTime(sArrDateT);
//		cal.add(Calendar.MINUTE, arrDelay);
//		Date aArrDateT = cal.getTime();     //actual arrival time and date
//		
//		if (aDeptDateT.after(aArrDateT)) {
//			System.out.println("actual departure time is after actual arrival time.");
//			return false;
//		}

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

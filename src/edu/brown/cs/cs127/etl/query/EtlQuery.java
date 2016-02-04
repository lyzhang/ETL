package edu.brown.cs.cs127.etl.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class EtlQuery {
	private Connection conn;

	public EtlQuery(String pathToDatabase) throws Exception {
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:" + pathToDatabase);

		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
	}

	public ResultSet query1(String[] args) throws SQLException {
		/**
		 * For some sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321
		 * /http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn
				.prepareStatement("select Code from Airport order by Code asc");

		return stat.executeQuery();
	}

	public ResultSet query2(String[] args) throws SQLException {
		PreparedStatement stat = conn
				.prepareStatement("select Code from Airline order by Code asc");

		return stat.executeQuery();
	}

	public ResultSet query3(String[] args) throws SQLException {
		PreparedStatement stat = conn
				.prepareStatement("select count(*) from Flight");

		return stat.executeQuery();
	}

	public ResultSet query4(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
				"select Reason, count(*) as count " 
				+ "from DelayReason "  
				+ "group by Reason "
				+ "order by count desc; ");			

		return stat.executeQuery();
	}
	
	public ResultSet query5(String[] args) throws SQLException
	{
		String airlineCode = args[0];
		String flightNum = args[1];
		String month = args[2];    // 1 = January, 2 = February, ..., 12 = December
		String day = args[3];     // 1, 2 ... 31
		String year = args[4];    //2010, 2011, 2012, etc
		
		if(month.length() == 1) {
			month = "0" + month;
		}
		
		if(day.length() == 1) {
			day = "0" + day;
		}
		 
		String date = year + "-" + month  + "-" + day;		
				
		PreparedStatement stat = conn.prepareStatement(
				"select DeptAirportCode, ArrAirportCode, sDeptDate || ' ' || sDeptTime  as sDeptDateTime " +
				" from Flight where AirlineCode = ? and FlightNum = ? and sDeptDate = ? ; "); 		
		
		stat.setString(1, airlineCode);
		stat.setString(2, flightNum);
		stat.setString(3, date);
		return stat.executeQuery();
	}	
	
	public ResultSet query6(String[] args) throws SQLException
	{
		String month = args[0];    // 1 = January, 2 = February, ..., 12 = December
		String day = args[1];     // 1, 2 ... 31
		String year = args[2];    //2010, 2011, 2012, etc
		
		if(month.length() == 1) {
			month = "0" + month;
		}
		
		if(day.length() == 1) {
			day = "0" + day;
		}
		 
		String date = year + "-" + month  + "-" + day;

		PreparedStatement stat = conn.prepareStatement("select Name, count(ID) as count " +
				"from Airline left outer natural join (select AirlineCode as Code, ID " +
				"from Flight " +
				"where sDeptDate = ? " +
				") " +
				"group by Code " +
				"order by count desc, Name asc " ); 
				
		stat.setString(1, date);
		return stat.executeQuery();		
				
	}
	
	public ResultSet query7(String[] args) throws SQLException
	{
		String month = args[0];    // 1 = January, 2 = February, ..., 12 = December
		String day = args[1];     // 1, 2 ... 31
		String year = args[2];    //2010, 2011, 2012, etc
		
		if(month.length() == 1) {
			month = "0" + month;
		}
		
		if(day.length() == 1) {
			day = "0" + day;
		}
		 
		String date = year + "-" + month  + "-" + day;
		
		int numOfAirports = args.length - 3;
		String airportStr = "?"; 
		
		for(int i = 1; i < numOfAirports; i++ ) {
			airportStr += ",?";
		}
			
		String sql = "select * from " +
				"( " +
				"select Name, count(ID) as DeptCount " +
				"from ( select Code as DeptAirportCode, Name " +
				"from Airport " +
				"where Name in (" + airportStr + ") " +
				") left outer natural join " +
				"(select * from Flight where sDeptDate = ?) " +
				"group by Name " +
				"order by Name asc " +
				") " +
				"natural join " +
				"( " +
				"select Name, count(ID) as ArrCount " +
				"from ( select Code as ArrAirportCode, Name " +
				"from Airport " +
				"where Name in (" + airportStr + ") " +
				") left outer natural join " +
				"(select * from Flight where sArrDate = ?) " +
				"group by Name " +
				"order by Name asc " +
				") order by Name asc " ;
		
		PreparedStatement stat = conn.prepareStatement(sql);
		for(int i = 1; i <= numOfAirports; i++) {
			stat.setString(i, args[2+i]);
		}
		stat.setString(numOfAirports+1, date);
		
		
		for(int i = 1; i <= numOfAirports; i++) {
			stat.setString(1+numOfAirports+i, args[2+i]);
		}
		stat.setString(1+numOfAirports+numOfAirports+1, date);
		
		return stat.executeQuery();					
	}	
	
	public ResultSet query8(String[] args) throws SQLException
	{
		String city = args[0];
		String State = args[1];
		String startDate = args[2];
		String endDate = args[3];
		try {
			startDate = standardlizeDate(startDate);
			endDate = standardlizeDate(endDate);
		} catch (ParseException e) {
			System.err.println("Error: Input Date format is wrong in query 8");
		}
		String sql = "select Name from " +
				"Airline join ( " +
				"select AirlineCode, count(ID) as count " +
				"from " +
				"( " +
				"select * from " +
				"(select distinct AirlineCode " +
				"from " +
				"Airport " +
				"join " +
				"( " +
				"select * from " +
				"Flight where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
				")  " +
				"on Airport.Code = DeptAirportCode " +
				"where City = ? and State = ? " +
				") left outer natural join  " +
				"( select * from " +
				"( " +
				"select * " +
				"from " +
				"Airport " +
				"join " +
				"( " +
				"select * from " +
				"Flight where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
				")  " +
				"on Airport.Code = DeptAirportCode " +
				"where City = ? and State = ? " +
				") natural join CanceledFlight " +
				") " +
				")  group by AirlineCode ) " +
				"on Airline.Code = AirlineCode " +
				"order by count desc, Name asc " +
				"limit 1 ";
		PreparedStatement stat = conn.prepareStatement(sql);
		stat.setString(1, startDate);
		stat.setString(2, endDate);
		stat.setString(3, city);
		stat.setString(4, State);
		stat.setString(5, startDate);
		stat.setString(6, endDate);
		stat.setString(7, city);
		stat.setString(8, State);		
		return stat.executeQuery();		
	}
	

	public ResultSet query9(String[] args) throws SQLException
	{
		String airlineName = args[0];
		String flightNum = args[1];
		String startDate = args[2];
		String endDate = args[3];
		try {
			startDate = standardlizeDate(startDate);
			endDate = standardlizeDate(endDate);
		} catch (ParseException e) {
			System.err.println("Error: Input Date format is wrong in query 9");
		}
		String sql =  "select * " +
					  "from ( " +
						"select count(*) FlightScheduled " +
						"from Flight join Airline on Airline.Code = Flight.AirlineCode " +
						"where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
						"and Name = ? and FlightNum =  ? " +
						") natural join ( " + 
						"select count(*) as CanceledFlight from CanceledFlight natural join ( " +
						"select * from " +
						"Flight join Airline on Airline.Code = Flight.AirlineCode " + 
						"where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
						"and Name = ? and FlightNum =  ?) " +
						") natural join ( " +
						"select count(*) as EarlyDeparted " +
						"from DelayedFlight natural join " +
						"( select * from " +
						"Flight join Airline on Airline.Code = Flight.AirlineCode " +
						"where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
						"and Name = ? and FlightNum =  ? " +
						") where DeptDelay <= 0 " +
						") natural join ( " +
						"select count(*) as LateDeparted " +
						"from DelayedFlight natural join " +
						"( select * from " +
						"Flight join Airline on Airline.Code = Flight.AirlineCode " +
						"where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
						"and Name = ? and FlightNum =  ? " +
						") where DeptDelay > 0 " +
						") natural join ( " +
						"select count(*) as EarlyArrived " +
						"from DelayedFlight natural join " +
						"( select * from " +
						"Flight join Airline on Airline.Code = Flight.AirlineCode " +
						"where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
						"and Name = ? and FlightNum =  ? " +
						") where ArrDelay <= 0 " +
						") natural join ( " +
						"select count(*) as LateArrived " +
						"from DelayedFlight natural join " +
						"( select * from " +
						"Flight join Airline on Airline.Code = Flight.AirlineCode " +
						"where ( (julianday(sDeptDate) >= julianday(?)) and (julianday(sDeptDate) <= julianday(?) )) " +
						"and Name = ? and FlightNum =  ? " +
						") where ArrDelay > 0 " +
						") " ;
 
				
		PreparedStatement stat = conn.prepareStatement(sql);
		stat.setString(1, startDate);
		stat.setString(5, startDate);
		stat.setString(9, startDate);
		stat.setString(13, startDate);
		stat.setString(17, startDate);
		stat.setString(21, startDate);
		
		stat.setString(2, endDate);
		stat.setString(6, endDate);
		stat.setString(10, endDate);
		stat.setString(14, endDate);
		stat.setString(18, endDate);
		stat.setString(22, endDate);
		
		stat.setString(3, airlineName);
		stat.setString(7, airlineName);
		stat.setString(11, airlineName);
		stat.setString(15, airlineName);
		stat.setString(19, airlineName);
		stat.setString(23, airlineName);
		
		stat.setString(4, flightNum);
		stat.setString(8, flightNum);
		stat.setString(12, flightNum);
		stat.setString(16, flightNum);
		stat.setString(20, flightNum);
		stat.setString(24, flightNum);
		
		return stat.executeQuery();		
	}
	
	public ResultSet query10(String[] args) throws SQLException
	{
		String deptCity = args[0];
		String deptState = args[1];
		String arrCity = args[2];
		String arrState = args[3];
		String date = args[4];

		try {
			date = standardlizeDate(date);			
		} catch (ParseException e) {
			System.err.println("Error: Input Date format is wrong in query 10");
		}
		
		String sql = "select AirlineCode, FlightNum, DeptAirportCode, strftime('%H:%M', aDeptTime) as aDeptTime, ArrAirportCode, strftime('%H:%M', aArrTime) as aArrTime, Duration from ( " +
				"select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, " + 
				"date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, " + 
				"time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime, " +  
				"date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, " + 
				"time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime, " +
				"((strftime('%s',sArrDate || ' ' || sArrTime)/60 + ArrDelay - strftime('%s',sDeptDate || ' ' || sDeptTime)/60 - DeptDelay )) as Duration " +
				"from ( " +
				"select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"Airport join Flight " + 
				"on Airport.Code = Flight.DeptAirportCode " +
				"where Airport.City = ? and Airport.State = ?) " +
				"join Airport on ArrAirportCode = Airport.Code " +
				"where Airport.City = ? and Airport.State = ? " +
				") natural join DelayedFlight " +
				"where aDeptDate = ? and aArrDate = ? " +
				") order by Duration asc " ;
		PreparedStatement stat = conn.prepareStatement(sql);
		stat.setString(1, deptCity);
		stat.setString(2, deptState);
		stat.setString(3, arrCity);
		stat.setString(4, arrState);
		stat.setString(5, date);
		stat.setString(6, date);
		
		return stat.executeQuery();		
	}
	
	public ResultSet query11(String[] args) throws SQLException
	{
		String deptCity = args[0];
		String deptState = args[1];
		String arrCity = args[2];
		String arrState = args[3];
		String date = args[4];

		try {
			date = standardlizeDate(date);			
		} catch (ParseException e) {
			System.err.println("Error: Input Date format is wrong in query 11");
		}
		
		String sql = "select H1AirlineCode,  H1FlightNum, H1DeptAirportCode, H1aDeptTime, H1ArrAirportCode, H1aArrTime, " +
				"H2AirlineCode,  H2FlightNum, H2DeptAirportCode, H2aDeptTime, H2ArrAirportCode, H2aArrTime, " +
				"strftime('%s', H2aArrDate || ' ' || H2aArrTime)/60 - strftime('%s', H1aDeptDate || ' ' || H1aDeptTime)/60 as Duration " +
				"from ( " +
				"select AirlineCode as H1AirlineCode, FlightNum as H1FlightNum,  " +
				"DeptAirportCode as H1DeptAirportCode, strftime('%H:%M', aDeptTime) as H1aDeptTime, aDeptDate as H1aDeptDate, " +
				"ArrAirportCode as H1ArrAirportCode, strftime('%H:%M', aArrTime) as H1aArrTime, aArrDate as H1aArrDate " +
				"from ( " +
				"select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode,  " +
				"date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate,  " +
				"time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,   " +
				"date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate,  " +
				"time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime " +
				"from ( " +
				"select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"Airport join Flight  " +
				"on Airport.Code = Flight.DeptAirportCode " +
				"where Airport.City = ? and Airport.State = ? ) " +
				"join Airport on ArrAirportCode = Airport.Code where ArrCity <> ? or ArrState <> ? " +
				") natural join DelayedFlight " +
				"where aDeptDate = ? and aArrDate = ? " +
				") " +
				") " +
				"join " +
				"( " +
				"select AirlineCode as H2AirlineCode , FlightNum as H2FlightNum,  " +
				"DeptAirportCode as H2DeptAirportCode, strftime('%H:%M', aDeptTime) as H2aDeptTime, aDeptDate as H2aDeptDate, " +
				"ArrAirportCode as H2ArrAirportCode, strftime('%H:%M', aArrTime) as H2aArrTime, aArrDate as H2aArrDate " +
				"from ( " +
				"select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode,  " +
				"date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate,  " +
				"time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,   " +
				"date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate,  " +
				"time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime " +
				"from ( " +
				"select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"Airport join Flight  " +
				"on Airport.Code = Flight.DeptAirportCode where DeptCity <> ? or DeptState <> ?) " +
				"join Airport on ArrAirportCode = Airport.Code " +
				"where Airport.City = ? and Airport.State = ? " +
				") natural join DelayedFlight " +
				"where aDeptDate = ? and aArrDate = ? " +
				")) " +
				"on H1ArrAirportCode = H2DeptAirportCode  " +
				"where julianday(H1aArrTime) < julianday(H2aDeptTime) " +
				"order by Duration asc ";
				
		PreparedStatement stat = conn.prepareStatement(sql);
		stat.setString(1, deptCity);
		stat.setString(2, deptState);
		stat.setString(3, arrCity);
		stat.setString(4, arrState);
		stat.setString(5, date);
		stat.setString(6, date);
		
		stat.setString(7, deptCity);
		stat.setString(8, deptState);	
		stat.setString(9, arrCity);
		stat.setString(10, arrState);
		stat.setString(11, date);
		stat.setString(12, date);
		
		return stat.executeQuery();		
	}
		

	public ResultSet query12(String[] args) throws SQLException
	{
		String deptCity = args[0];
		String deptState = args[1];
		String arrCity = args[2];
		String arrState = args[3];
		String date = args[4];

		try {
			date = standardlizeDate(date);			
		} catch (ParseException e) {
			System.err.println("Error: Input Date format is wrong in query 12");
		}
		
		String sql1 = "create temp table tmpFlight2 as " +
				"select H2ID, H2AirlineCode, H2FlightNum, H2DeptAirportCode, H2ArrAirportCode, H2DeptCity, H2DeptState, H2ArrCity, H2ArrState,  " +
				"strftime('%H:%M', H2aDeptDateTime) as H2aDeptTime, strftime('%H:%M', H2aArrDateTime) as H2aArrTime, " +
				"date(H2aDeptDateTime) as H2aDeptDate, date(H2aArrDateTime) as H2aArrDate,  " +
				"strftime('%H:%M', H2sDeptDateTime) as H2sDeptTime, strftime('%H:%M', H2sArrDateTime) as H2sArrTime, " +
				"date(H2sDeptDateTime) as H2sDeptDate, date(H2sArrDateTime) as H2sArrDate " +
				"from( " +
				"select ID as H2ID, AirlineCode as H2AirlineCode, FlightNum as H2FlightNum, DeptAirportCode as H2DeptAirportCode,  " +
				"ArrAirportCode as H2ArrAirportCode, DeptCity as H2DeptCity, DeptState as H2DeptState, ArrCity as H2ArrCity, ArrState as H2ArrState, " +
				"(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as H2aDeptDateTime,    " +
				"(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as H2aArrDateTime,  " +
				"julianday(sDeptDate || ' ' || sDeptTime) as H2sDeptDateTime, " +
				"julianday(sArrDate || ' ' || sArrTime) as H2sArrDateTime " +
				"from ( " +
				"select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from " +
				"Airport join Flight  " +
				"on Airport.Code = Flight.DeptAirportCode) " +
				"join Airport on ArrAirportCode = Airport.Code " +
				") natural join DelayedFlight " +
				"where date(H2aDeptDateTime) = ? and date(H2aArrDateTime) = ? and " +
				"H2aDeptDateTime < H2aArrDateTime and H2sDeptDateTime < H2sArrDateTime " +
				") ";
				
		PreparedStatement stat = conn.prepareStatement(sql1);
		stat.setString(1, date);
		stat.setString(2, date);
		stat.executeUpdate();		
		
		String sql2 = "select H1AirlineCode,  H1FlightNum, H1DeptAirportCode, H1aDeptTime, H1ArrAirportCode, H1aArrTime, " +
				"H2AirlineCode,  H2FlightNum, H2DeptAirportCode, H2aDeptTime, H2ArrAirportCode, H2aArrTime, " +
				"H3AirlineCode,  H3FlightNum, H3DeptAirportCode, H3aDeptTime, H3ArrAirportCode, H3aArrTime, " +
				"strftime('%s', H3aArrDate || ' ' || H3aArrTime)/60 - strftime('%s', H1aDeptDate || ' ' || H1aDeptTime)/60 as Duration " +
				"from ( " +
				"select * from (select H2ID as H1ID, H2AirlineCode as H1AirlineCode, H2FlightNum as H1FlightNum, H2DeptAirportCode as H1DeptAirportCode,  " +
				"H2ArrAirportCode as H1ArrAirportCode, H2DeptCity as H1DeptCity, H2DeptState as H1DeptState, H2ArrCity as H1ArrCity, H2ArrState as H1ArrState, " +
				"H2aDeptTime as H1aDeptTime, H2aArrTime as H1aArrTime, " +
				"H2aDeptDate as H1aDeptDate, H2aArrDate as H1aArrDate, " +
				"H2sDeptTime as H1sDeptTime, H2sArrTime as H1sArrTime, " +
				"H2sDeptDate as H1sDeptDate, H2sArrDate as H1sArrDate " +
				"from " +
				"tmpFlight2 " +
				"where H2DeptCity = ? and H2DeptState = ?  " +
				"and ( H2ArrCity <> ? or H2ArrState <> ?) " +
				"and ( H2ArrCity <> ? or H2ArrState <> ?) " +
				") join tmpFlight2 on H1ArrAirportCode = H2DeptAirportCode " +
				"where strftime('%s', H1aArrDate || ' ' || H1aArrTime) < strftime('%s', H2aDeptDate || ' ' || H2aDeptTime) " +
				"and H2ArrAirportCode <> H1DeptAirportCode " +
				") join ( " +
				"select H2ID as H3ID, H2AirlineCode as H3AirlineCode, H2FlightNum as H3FlightNum, H2DeptAirportCode as H3DeptAirportCode,  " +
				"H2ArrAirportCode as H3ArrAirportCode, H2DeptCity as H3DeptCity, H2DeptState as H3DeptState, H2ArrCity as H3ArrCity, H2ArrState as H3ArrState, " +
				"H2aDeptTime as H3aDeptTime, H2aArrTime as H3aArrTime, " +
				"H2aDeptDate as H3aDeptDate, H2aArrDate as H3aArrDate, " +
				"H2sDeptTime as H3sDeptTime, H2sArrTime as H3sArrTime, " +
				"H2sDeptDate as H3sDeptDate, H2sArrDate as H3sArrDate " +
				"from " +
				"tmpFlight2 " +
				"where H2ArrCity = ? and H2ArrState = ?  " +
				"and ( H2DeptCity <> ? or H2DeptState <> ? ) " +
				"and ( H2DeptCity <> ? or H2DeptState <> ?) " +
				") on H2ArrAirportCode = H3DeptAirportCode  " +
				"where strftime('%s', H2aArrDate || ' ' || H2aArrTime) < strftime('%s', H3aDeptDate || ' ' || H3aDeptTime) " +
				"and H3ArrAirportCode <> H2DeptAirportCode and H3ArrAirportCode <> H1DeptAirportCode " +
				"order by duration asc";

				
		stat = conn.prepareStatement(sql2);
		
		stat.setString(1, deptCity);
		stat.setString(2, deptState);		
		stat.setString(3, arrCity);
		stat.setString(4, arrState);
		stat.setString(5, deptCity);
		stat.setString(6, deptState);
		
		stat.setString(7, arrCity);
		stat.setString(8, arrState);
		stat.setString(9, arrCity);
		stat.setString(10, arrState);
		stat.setString(11, deptCity);
		stat.setString(12, deptState);
		
		
		return stat.executeQuery();		
	}
	
	private String standardlizeDate(String inputDate) throws ParseException {
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

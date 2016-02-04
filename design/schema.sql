PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;

CREATE TABLE Airport (
Code	varchar(5),
Name	varchar(100) not null,
City	varchar(20),
State	varchar(15),
primary key(Code)
);

CREATE TABLE Airline (
Code varchar(8),
Name varchar(60) not null,
primary key(Code)
);

CREATE TABLE FlightSchedule(
ID int,
AirlineCode varchar(8) not null,
FlightNum varchar(8) not null,
DeptAirportCode varchar(5) not null,
ArrAirportCode varchar(5) not null,
sDeptTime text not null,
sArrTime text not null,
sArrDay int not null,
primary key(ID),
foreign key(AirlineCode) references Airline,
foreign key(DeptAirportCode) references Airport,
foreign key(ArrAirportCode) references Airport,
check( time(sDeptTime) < time(sArrTime)  or sArrDay > 0)
);

CREATE TABLE CanceledFlight (
ID int,
sDeptDate text not null,
primary key(ID, sDeptDate),
foreign key(ID) references FlightSchedule
);

CREATE TABLE FlightRecord (
ID int,
sDeptDate text not null, 
DeptDelay int not null,   
ArrDelay int not null,
primary key(ID, sDeptDate),
foreign key(ID) references FlightSchedule
);

CREATE TABLE DelaySource (
ID int,
sDeptDate text not null,
Name text check( Name in ('CarrierDelay',  'WeatherDelay',  'AirTrafficDelay',  'SecurityDelay')),
DelayMinutes int not null, 
primary key(ID, sDeptDate, Name),
foreign key(ID, sDeptDate) references FlightRecord,
check( DelayMinutes >= 0)
);

COMMIT;

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

CREATE TABLE Flight(
ID int,
AirlineCode varchar(8) not null,
FlightNum varchar(8) not null,
DeptAirportCode varchar(5) not null,
ArrAirportCode varchar(5) not null,
sDeptDate text not null,
sDeptTime text not null,
sArrDate text not null,
sArrTime text not null,
primary key(ID),
foreign key(AirlineCode) references Airline,
foreign key(DeptAirportCode) references Airport,
foreign key(ArrAirportCode) references Airport,
check ( julianday (sDeptDate || ' ' || sDeptTime ) < julianday (sArrDate || ' ' || sArrTime ) )
);

CREATE TABLE CanceledFlight (
ID int,
primary key(ID),
foreign key(ID) references Flight
);

CREATE TABLE DelayedFlight (
ID int, 
DeptDelay int not null,   
ArrDelay int not null,
primary key(ID),
foreign key(ID) references Flight
);

CREATE TABLE DelayReason (
ID int,
Reason text check( Reason in ('CarrierDelay',  'WeatherDelay',  'AirTrafficDelay',  'SecurityDelay')),
DelayMinutes int not null, 
primary key(ID, Reason),
foreign key(ID) references DelayedFlight,
check( DelayMinutes > 0)
);

COMMIT;

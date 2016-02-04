
ALTER TABLE tmpFlight ADD COLUMN sArrDay int;
update tmpFlight set sArrDay = julianday(sArrDate) -  julianday(sDeptDate) 

create table tmpFlightSchedule as SELECT distinct AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, sDeptTime, sArrTime, sArrDay FROM tmpFlight
ALTER TABLE tmpFlightSchedule ADD COLUMN ID int;
update tmpFlightSchedule set ID = rowid;
insert into FlightSchedule select ID, AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, sDeptTime, sArrTime, sArrDay from tmpFlightSchedule

insert into CanceledFlight
select FlightSchedule.ID, tmpFlight.sDeptDate
from FlightSchedule join tmpFlight
on FlightSchedule.AirlineCode = tmpFlight.AirlineCode and  
FlightSchedule.FlightNum = tmpFlight.FlightNum and
FlightSchedule.DeptAirportCode = tmpFlight.DeptAirportCode and
FlightSchedule.ArrAirportCode = tmpFlight.ArrAirportCode and
FlightSchedule.sDeptTime = tmpFlight.sDeptTime and
FlightSchedule.sArrTime = tmpFlight.sArrTime and
FlightSchedule.sArrDay = tmpFlight.sArrDay 
where tmpFlight.Canceled = 1


insert into FlightRecord 
select FlightSchedule.ID, tmpFlight.sDeptDate, tmpFlight.DeptDelay, tmpFlight.ArrDelay
from FlightSchedule join tmpFlight
on FlightSchedule.AirlineCode = tmpFlight.AirlineCode and  
FlightSchedule.FlightNum = tmpFlight.FlightNum and
FlightSchedule.DeptAirportCode = tmpFlight.DeptAirportCode and
FlightSchedule.ArrAirportCode = tmpFlight.ArrAirportCode and
FlightSchedule.sDeptTime = tmpFlight.sDeptTime and
FlightSchedule.sArrTime = tmpFlight.sArrTime and
FlightSchedule.sArrDay = tmpFlight.sArrDay 
where tmpFlight.Canceled = 0




create table tmpDelaySource1 as
select FlightSchedule.ID, tmpFlight.sDeptDate, tmpFlight.CarrierDelay
from FlightSchedule join tmpFlight
on FlightSchedule.AirlineCode = tmpFlight.AirlineCode and  
FlightSchedule.FlightNum = tmpFlight.FlightNum and
FlightSchedule.DeptAirportCode = tmpFlight.DeptAirportCode and
FlightSchedule.ArrAirportCode = tmpFlight.ArrAirportCode and
FlightSchedule.sDeptTime = tmpFlight.sDeptTime and
FlightSchedule.sArrTime = tmpFlight.sArrTime and
FlightSchedule.sArrDay = tmpFlight.sArrDay 
where tmpFlight.Canceled = 0 and tmpFlight.CarrierDelay > 0

ALTER TABLE tmpDelaySource1 ADD COLUMN Name text;
update tmpDelaySource1 set Name = 'CarrierDelay';


create table tmpDelaySource2 as
select FlightSchedule.ID, tmpFlight.sDeptDate, tmpFlight.WeatherDelay
from FlightSchedule join tmpFlight
on FlightSchedule.AirlineCode = tmpFlight.AirlineCode and  
FlightSchedule.FlightNum = tmpFlight.FlightNum and
FlightSchedule.DeptAirportCode = tmpFlight.DeptAirportCode and
FlightSchedule.ArrAirportCode = tmpFlight.ArrAirportCode and
FlightSchedule.sDeptTime = tmpFlight.sDeptTime and
FlightSchedule.sArrTime = tmpFlight.sArrTime and
FlightSchedule.sArrDay = tmpFlight.sArrDay 
where tmpFlight.Canceled = 0 and tmpFlight.WeatherDelay > 0

ALTER TABLE tmpDelaySource2 ADD COLUMN Name text;
update tmpDelaySource2 set Name = 'WeatherDelay';


create table tmpDelaySource3 as
select FlightSchedule.ID, tmpFlight.sDeptDate, tmpFlight.AirTrafficDelay
from FlightSchedule join tmpFlight
on FlightSchedule.AirlineCode = tmpFlight.AirlineCode and  
FlightSchedule.FlightNum = tmpFlight.FlightNum and
FlightSchedule.DeptAirportCode = tmpFlight.DeptAirportCode and
FlightSchedule.ArrAirportCode = tmpFlight.ArrAirportCode and
FlightSchedule.sDeptTime = tmpFlight.sDeptTime and
FlightSchedule.sArrTime = tmpFlight.sArrTime and
FlightSchedule.sArrDay = tmpFlight.sArrDay 
where tmpFlight.Canceled = 0 and tmpFlight.AirTrafficDelay > 0

ALTER TABLE tmpDelaySource3 ADD COLUMN Name text;
update tmpDelaySource3 set Name = 'AirTrafficDelay';



create table tmpDelaySource4 as
select FlightSchedule.ID, tmpFlight.sDeptDate, tmpFlight.securityDelay
from FlightSchedule join tmpFlight
on FlightSchedule.AirlineCode = tmpFlight.AirlineCode and  
FlightSchedule.FlightNum = tmpFlight.FlightNum and
FlightSchedule.DeptAirportCode = tmpFlight.DeptAirportCode and
FlightSchedule.ArrAirportCode = tmpFlight.ArrAirportCode and
FlightSchedule.sDeptTime = tmpFlight.sDeptTime and
FlightSchedule.sArrTime = tmpFlight.sArrTime and
FlightSchedule.sArrDay = tmpFlight.sArrDay 
where tmpFlight.Canceled = 0 and tmpFlight.securityDelay > 0

ALTER TABLE tmpDelaySource4 ADD COLUMN Name text;
update tmpDelaySource4 set Name = 'SecurityDelay';



insert into DelaySource select ID, sDeptDate, Name, CarrierDelay as DelayMinutes from tmpDelaySource1
insert into DelaySource select ID, sDeptDate, Name, WeatherDelay as DelayMinutes from tmpDelaySource2
insert into DelaySource select ID, sDeptDate, Name, AirTrafficDelay as DelayMinutes from tmpDelaySource3
insert into DelaySource select ID, sDeptDate, Name, securityDelay as DelayMinutes from tmpDelaySource4

drop table if exists tmpDelaySource1
drop table if exists tmpDelaySource2
drop table if exists tmpDelaySource3
drop table if exists tmpDelaySource4
drop table if exists tmpFlightSchedule









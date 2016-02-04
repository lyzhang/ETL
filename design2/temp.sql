
create table tmpFlight as SELECT AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime FROM tmpFlight
ALTER TABLE tmpFlight ADD COLUMN ID int;
update tmpFlight set ID = rowid;
insert into Flight select ID, AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from tmpFlight

insert into CanceledFlight
select Flight.ID
from Flight join tmpFlight
on Flight.AirlineCode = tmpFlight.AirlineCode and  
Flight.FlightNum = tmpFlight.FlightNum and
Flight.DeptAirportCode = tmpFlight.DeptAirportCode and
Flight.ArrAirportCode = tmpFlight.ArrAirportCode and
Flight.sDeptDate = tmpFlight.sDeptDate and
Flight.sDeptTime = tmpFlight.sDeptTime and
Flight.sArrDate = tmpFlight.sArrDate and
Flight.sArrTime = tmpFlight.sArrTime
where tmpFlight.Canceled = 1


insert into DelayedFlight 
select Flight.ID, tmpFlight.DeptDelay, tmpFlight.ArrDelay
from Flight join tmpFlight
on Flight.AirlineCode = tmpFlight.AirlineCode and  
Flight.FlightNum = tmpFlight.FlightNum and
Flight.DeptAirportCode = tmpFlight.DeptAirportCode and
Flight.ArrAirportCode = tmpFlight.ArrAirportCode and
Flight.sDeptDate = tmpFlight.sDeptDate and
Flight.sDeptTime = tmpFlight.sDeptTime and
Flight.sArrDate = tmpFlight.sArrDate and
Flight.sArrTime = tmpFlight.sArrTime 
where tmpFlight.Canceled = 0




create table tmpDelaySource1 as
select Flight.ID, tmpFlight.CarrierDelay
from Flight join tmpFlight
on Flight.AirlineCode = tmpFlight.AirlineCode and  
Flight.FlightNum = tmpFlight.FlightNum and
Flight.DeptAirportCode = tmpFlight.DeptAirportCode and
Flight.ArrAirportCode = tmpFlight.ArrAirportCode and
Flight.sDeptDate = tmpFlight.sDeptDate and
Flight.sDeptTime = tmpFlight.sDeptTime and
Flight.sArrDate = tmpFlight.sArrDate and
Flight.sArrTime = tmpFlight.sArrTime
where tmpFlight.Canceled = 0 and tmpFlight.CarrierDelay > 0

ALTER TABLE tmpDelaySource1 ADD COLUMN Reason text;
update tmpDelaySource1 set Reason = 'CarrierDelay';


create table tmpDelaySource2 as
select Flight.ID,  tmpFlight.WeatherDelay
from Flight join tmpFlight
on Flight.AirlineCode = tmpFlight.AirlineCode and  
Flight.FlightNum = tmpFlight.FlightNum and
Flight.DeptAirportCode = tmpFlight.DeptAirportCode and
Flight.ArrAirportCode = tmpFlight.ArrAirportCode and
Flight.sDeptDate = tmpFlight.sDeptDate and
Flight.sDeptTime = tmpFlight.sDeptTime and
Flight.sArrDate = tmpFlight.sArrDate and
Flight.sArrTime = tmpFlight.sArrTime
where tmpFlight.Canceled = 0 and tmpFlight.WeatherDelay > 0

ALTER TABLE tmpDelaySource2 ADD COLUMN Reason text;
update tmpDelaySource2 set Reason = 'WeatherDelay';


create table tmpDelaySource3 as
select Flight.ID, tmpFlight.AirTrafficDelay
from Flight join tmpFlight
on Flight.AirlineCode = tmpFlight.AirlineCode and  
Flight.FlightNum = tmpFlight.FlightNum and
Flight.DeptAirportCode = tmpFlight.DeptAirportCode and
Flight.ArrAirportCode = tmpFlight.ArrAirportCode and
Flight.sDeptDate = tmpFlight.sDeptDate and
Flight.sDeptTime = tmpFlight.sDeptTime and
Flight.sArrDate = tmpFlight.sArrDate and
Flight.sArrTime = tmpFlight.sArrTime 
where tmpFlight.Canceled = 0 and tmpFlight.AirTrafficDelay > 0

ALTER TABLE tmpDelaySource3 ADD COLUMN Reason text;
update tmpDelaySource3 set Reason = 'AirTrafficDelay';



create table tmpDelaySource4 as
select Flight.ID, tmpFlight.securityDelay
from Flight join tmpFlight
on Flight.AirlineCode = tmpFlight.AirlineCode and  
Flight.FlightNum = tmpFlight.FlightNum and
Flight.DeptAirportCode = tmpFlight.DeptAirportCode and
Flight.ArrAirportCode = tmpFlight.ArrAirportCode and
Flight.sDeptDate = tmpFlight.sDeptDate and
Flight.sDeptTime = tmpFlight.sDeptTime and
Flight.sArrDate = tmpFlight.sArrDate and
Flight.sArrTime = tmpFlight.sArrTime
where tmpFlight.Canceled = 0 and tmpFlight.securityDelay > 0

ALTER TABLE tmpDelaySource4 ADD COLUMN Reason text;
update tmpDelaySource4 set Reason = 'SecurityDelay';



insert into DelaySource select ID, Reason, CarrierDelay as DelayMinutes from tmpDelaySource1
insert into DelaySource select ID, Reason, WeatherDelay as DelayMinutes from tmpDelaySource2
insert into DelaySource select ID, Reason, AirTrafficDelay as DelayMinutes from tmpDelaySource3
insert into DelaySource select ID, Reason, securityDelay as DelayMinutes from tmpDelaySource4

drop table if exists tmpDelaySource1
drop table if exists tmpDelaySource2
drop table if exists tmpDelaySource3
drop table if exists tmpDelaySource4
drop table if exists tmpFlight









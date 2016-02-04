 1. Get all airport codes in alphabetical order.
Input: N/A
Output: One column. airport ID.
A:
select Code from Airport order by Code asc

2. Get all airline codes in alphabetical order.
Input: N/A
Output: One column. airline ID.
A:
select Code from Airline order by Code asc

3.
Count the number of total flights.
Input: N/A
Output: One column. Number of flights.
A:
select count(*) from Flight

4. Get all the reasons flights were delayed, along with their frequency, in order from
highest frequency to lowest.
Input: N/A
Output: Two columns. The first column should be a string describing the type of
delay. The four types of delays are Carrier Delay, Weather Delay, Air Traffic
Delay, and Security Delay. The second column should be the number of flights
that experienced that type of delay. The results should be in order from largest
number of flights to smallest.
A:
select Reason, count(*) as count
from DelayReason 
group by Reason
order by count desc

5. Return details for a specified airline code and flight number scheduled to depart on a
particular day.
Input 1: An airline code (eg: AA)
Input 2: A flight number.
Input 3: A month (1 = January, 2 = February, ..., 12 = December)
Input 4: A day (1, 2 ... 31)
Input 5: A year (2010, 2011, 2012, etc)
Output: Three columns. In this order: departing airport code, arriving airport code,
and scheduled date and time of departure (in format YYYY-MM-DD HH:MM).

A: 

select DeptAirportCode, ArrAirportCode, sDeptDate || ' ' || sDeptTime as sDeptDateTime
from Flight
where AirlineCode = 'AA' and FlightNum = '1140' and sDeptDate = '2012-01-20'


6. Get all airlines, along with the number of flights by that airline which were scheduled
to depart on a particular day (whether or not they departed). Results should be
ordered from highest frequency to lowest frequency.
Input 1: A month (1 = January, 2 = February, ..., 12 = December)
Input 2: A day (1, 2 ... 31)
Input 3: A year (2010, 2011, 2012, etc)
Output: Two columns. The first column should be the name of the airline. The
second column should be the number of flights matching the criteria.

A:

select Name, count(ID) as count
from Airline left outer natural join (select AirlineCode as Code, ID
from Flight
where sDeptDate = '2012-01-02'
)
group by Code
order by count desc, Name asc

 
7. For a specified set of airports, return the number of departing and the number of
arriving planes on a particular day (scheduled departures/arrivals). Results should
be ordered alphabetically by airport name, A-Z.
Input 1: A month (1 = January, 2 = February, ..., 12 = December)
Input 2: A day (1, 2 ... 31)
Input 3: A year (2010, 2011, 2012, etc)
Input 4 .. n: The full, canonical name of an airport (ie: LaGuardia).
Output: Three columns. The first column should be the name of the airport. The
second column should be the number of flights that were scheduled to depart the
airport on the specified day. The third column should be the number of flights
that were scheduled to arrive at the airport on the specified day.

Base1:
select Name, count(ID) as DeptCount
from ( select Code as DeptAirportCode, Name
from Airport 
where Name in ('LaGuardia','Watson Island International')
) left outer natural join
(select * from Flight where sDeptDate = '2012-01-02')
group by Name 
order by Name asc

Base2:
select Name, count(ID) as ArrCount
from ( select Code as ArrAirportCode, Name
from Airport 
where Name in ('LaGuardia','Watson Island International')
) left outer natural join
(select * from Flight where sArrDate = '2012-01-02')
group by Name 
order by Name asc


A:
select * from
(
select Name, count(ID) as DeptCount
from ( select Code as DeptAirportCode, Name
from Airport 
where Name in ('LaGuardia','Watson Island International')
) left outer natural join
(select * from Flight where sDeptDate = '2012-01-02')
group by Name 
order by Name asc
)
natural join
(
select Name, count(ID) as ArrCount
from ( select Code as ArrAirportCode, Name
from Airport 
where Name in ('LaGuardia','Watson Island International')
) left outer natural join
(select * from Flight where sArrDate = '2012-01-02')
group by Name 
order by Name asc
)
order by Name asc

"MPB","Watson Island International",,

8.
Which airline had the greatest number of cancelled flights scheduled to depart from
a specified city during a specified range of dates (inclusive of both start and end),
excluding airlines with no flights to that city. If there is a tie, return the airline which
comes first alphabetically (sorted A-Z).
Input 1: A city name (ie: Providence, Newark, etc).
Input 2: A state name (ie: Rhode Island, New York, etc).
Input 3: A start date, in MM/DD/YYYY format.
Input 4: An end date, in MM/DD/YYYY format.
Output: One column: the name of the airline matching the criteria.

base1:  Airline with flights to the city within that days range
select distinct AirlineCode
from
Airport
join
(
select * from
Flight where ( (julianday(sDeptDate) >= julianday('2012-01-14')) and (julianday(sDeptDate) <= julianday('2012-01-14') ))
) 
on Airport.Code = DeptAirportCode
where City = 'Providence' and State = 'Rhode Island'

A:

select Name from
Airline join (
select AirlineCode, count(ID) as count
from
(
select * from
(select distinct AirlineCode
from
Airport
join
(
select * from
Flight where ( (julianday(sDeptDate) >= julianday('2012-01-14')) and (julianday(sDeptDate) <= julianday('2012-01-14') ))
) 
on Airport.Code = DeptAirportCode
where City = 'Providence' and State = 'Rhode Island'
) left outer natural join 
( select * from
(
select *
from
Airport
join
(
select * from
Flight where ( (julianday(sDeptDate) >= julianday('2012-02-14')) and (julianday(sDeptDate) <= julianday('2012-02-14') ))
) 
on Airport.Code = DeptAirportCode
where City = 'Providence' and State = 'Rhode Island'
) natural join CanceledFlight
)
)  group by AirlineCode )
on Airline.Code = AirlineCode
order by count desc, Name asc
limit 1

9:
base:

select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'

column1:
select count(*) FlightScheduled
from Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'

column2:
select count(*) as CanceledFlight from CanceledFlight natural join (
select * from
Flight join Airline on Airline.Code = Flight.AirlineCode 
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050')

column3:
select count(*) as EarlyDeparted
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where DeptDelay <= 0

column4:
select count(*) as LateDeparted
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where DeptDelay > 0

column5:
select count(*) as EarlyArrived
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where ArrDelay <= 0

column6:
select count(*) as LateArrived
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where ArrDelay > 0

All:

select *
from (
select count(*) FlightScheduled
from Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) natural join (
select count(*) as CanceledFlight from CanceledFlight natural join (
select * from
Flight join Airline on Airline.Code = Flight.AirlineCode 
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050')
) natural join (
select count(*) as EarlyDeparted
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where DeptDelay <= 0
) natural join (
select count(*) as LateDeparted
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where DeptDelay > 0
) natural join (
select count(*) as EarlyArrived
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where ArrDelay <= 0
) natural join (
select count(*) as LateArrived
from DelayedFlight natural join
( select * from
Flight join Airline on Airline.Code = Flight.AirlineCode
where ( (julianday(sDeptDate) >= julianday('2012-01-10')) and (julianday(sDeptDate) <= julianday('2012-01-19') ))
and Name = 'American Airlines Inc.' and FlightNum =  '2050'
) where ArrDelay > 0
)

10:
base:

select * from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode
where Airport.City = 'Newark' and Airport.State = 'New Jersey' )
join Airport on ArrAirportCode = Airport.Code
where Airport.City = 'Chicago' and Airport.State = 'Illinois'
) natural join DelayedFlight

F:
select AirlineCode, FlightNum, DeptAirportCode, strftime('%H:%M', aDeptTime) as aDeptTime, ArrAirportCode, strftime('%H:%M', aArrTime) as aArrTime, Duration from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate,  
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime,
strftime('%s',sArrDate || ' ' || sArrTime)/60 + ArrDelay - strftime('%s',sDeptDate || ' ' || sDeptTime)/60 - DeptDelay  as Duration
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode
where Airport.City = 'Newark' and Airport.State = 'New Jersey' )
join Airport on ArrAirportCode = Airport.Code
where Airport.City = 'Chicago' and Airport.State = 'Illinois'
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
) order by Duration asc

11.

base1:
from startCity to anywhere:

select AirlineCode as H1AirlineCode, FlightNum as H1FlightNum, 
DeptAirportCode as H1DeptAirportCode, strftime('%H:%M', aDeptTime) as H1aDeptTime, aDeptDate as H1aDeptDate,
ArrAirportCode as H1ArrAirportCode, strftime('%H:%M', aArrTime) as H1aArrTime, aArrDate as H1aArrDate
from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode
where Airport.City = 'Newark' and Airport.State = 'New Jersey' )
join Airport on ArrAirportCode = Airport.Code  where(ArrCity <> 'Chicago' or ArrState <> 'Illinois')
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03' 
)

base2:
from anywhere to endCity:

select AirlineCode as H2AirlineCode , FlightNum as H2FlightNum, 
DeptAirportCode as H2DeptAirportCode, strftime('%H:%M', aDeptTime) as H2aDeptTime, aDeptDate as H2aDeptDate,
ArrAirportCode as H2ArrAirportCode, strftime('%H:%M', aArrTime) as H2aArrTime, aArrDate as H2aArrDate
from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode where (DeptCity <> 'Newark' or DeptState <> 'New Jersey') )
join Airport on ArrAirportCode = Airport.Code
where Airport.City = 'Chicago' and Airport.State = 'Illinois'
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)


Final:

select H1AirlineCode,  H1FlightNum, H1DeptAirportCode, H1aDeptTime, H1ArrAirportCode, H1aArrTime,
H2AirlineCode,  H2FlightNum, H2DeptAirportCode, H2aDeptTime, H2ArrAirportCode, H2aArrTime,
strftime('%s', H2aArrDate || ' ' || H2aArrTime)/60 - strftime('%s', H1aDeptDate || ' ' || H1aDeptTime)/60 as Duration
from (
select AirlineCode as H1AirlineCode, FlightNum as H1FlightNum, 
DeptAirportCode as H1DeptAirportCode, strftime('%H:%M', aDeptTime) as H1aDeptTime, aDeptDate as H1aDeptDate,
ArrAirportCode as H1ArrAirportCode, strftime('%H:%M', aArrTime) as H1aArrTime, aArrDate as H1aArrDate
from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode
where Airport.City = 'Newark' and Airport.State = 'New Jersey' )
join Airport on ArrAirportCode = Airport.Code  where(ArrCity <> 'Chicago' or ArrState <> 'Illinois')
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)
)
join
(
select AirlineCode as H2AirlineCode , FlightNum as H2FlightNum, 
DeptAirportCode as H2DeptAirportCode, strftime('%H:%M', aDeptTime) as H2aDeptTime, aDeptDate as H2aDeptDate,
ArrAirportCode as H2ArrAirportCode, strftime('%H:%M', aArrTime) as H2aArrTime, aArrDate as H2aArrDate
from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode where (DeptCity <> 'Newark' or DeptState <> 'New Jersey') )
join Airport on ArrAirportCode = Airport.Code
where Airport.City = 'Chicago' and Airport.State = 'Illinois'
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
))
on H1ArrAirportCode = H2DeptAirportCode 
where julianday(H1aArrTime) < julianday(H2aDeptTime)
order by Duration asc

12
base



h1: startCity to anywhere:


select AirlineCode as H1AirlineCode, FlightNum as H1FlightNum, DeptAirportCode as H1DeptAirportCode, strftime('%H:%M', aDeptTime) as H1aDeptTime, ArrAirportCode as H1ArrAirportCode, strftime('%H:%M', aArrTime) as H1aArrTime from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode
where Airport.City = 'Newark' and Airport.State = 'New Jersey' )
join Airport on ArrAirportCode = Airport.Code
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)

h2: anywhere to anywhere:

select AirlineCode as H2AirlineCode , FlightNum as H2FlightNum, DeptAirportCode as H2DeptAirportCode, strftime('%H:%M', aDeptTime) as H2aDeptTime, ArrAirportCode as H2ArrAirportCode, strftime('%H:%M', aArrTime) as H2aArrTime from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode)
join Airport on ArrAirportCode = Airport.Code
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)


h3: anywhere to endCity:

select AirlineCode as H3AirlineCode , FlightNum as H3FlightNum, DeptAirportCode as H3DeptAirportCode, strftime('%H:%M', aDeptTime) as H3aDeptTime, ArrAirportCode as H3ArrAirportCode, strftime('%H:%M', aArrTime) as H3aArrTime from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode)
join Airport on ArrAirportCode = Airport.Code
where Airport.City = 'Chicago' and Airport.State = 'Illinois'
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)


final:

select H1AirlineCode,  H1FlightNum, H1DeptAirportCode, H1aDeptTime, H1ArrAirportCode, H1aArrTime,
H2AirlineCode,  H2FlightNum, H2DeptAirportCode, H2aDeptTime, H2ArrAirportCode, H2aArrTime,
H3AirlineCode,  H3FlightNum, H3DeptAirportCode, H3aDeptTime, H3ArrAirportCode, H3aArrTime,
round(1440*(julianday(H3aArrTime) - julianday(H1aDeptTime) )) as Duration
from (
select * from
(
select AirlineCode as H1AirlineCode, FlightNum as H1FlightNum, DeptAirportCode as H1DeptAirportCode, strftime('%H:%M', aDeptTime) as H1aDeptTime, ArrAirportCode as H1ArrAirportCode, strftime('%H:%M', aArrTime) as H1aArrTime from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode
where Airport.City = 'Newark' and Airport.State = 'New Jersey' )
join Airport on ArrAirportCode = Airport.Code
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)
) join (
select AirlineCode as H2AirlineCode , FlightNum as H2FlightNum, DeptAirportCode as H2DeptAirportCode, strftime('%H:%M', aDeptTime) as H2aDeptTime, ArrAirportCode as H2ArrAirportCode, strftime('%H:%M', aArrTime) as H2aArrTime from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode)
join Airport on ArrAirportCode = Airport.Code
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)
) on H1ArrAirportCode = H2DeptAirportCode 
where julianday(H1aArrTime) < julianday(H2aDeptTime)
)
join (
select AirlineCode as H3AirlineCode , FlightNum as H3FlightNum, DeptAirportCode as H3DeptAirportCode, strftime('%H:%M', aDeptTime) as H3aDeptTime, ArrAirportCode as H3ArrAirportCode, strftime('%H:%M', aArrTime) as H3aArrTime from (
select AirlineCode, FlightNum, DeptAirportCode, ArrAirportCode, 
date(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptDate, 
time(julianday(sDeptDate || ' ' || sDeptTime) + (1.0*DeptDelay/1440) ) as aDeptTime,  
date(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrDate, 
time(julianday(sArrDate || ' ' || sArrTime) + (1.0*ArrDelay/1440) ) as aArrTime
from (
select ID, DeptAirportCode, DeptCity, DeptState, ArrAirportCode, City as ArrCity, State as ArrState, AirlineCode, FlightNum, sDeptDate, sDeptTime, sArrDate, sArrTime from
(select ID, DeptAirportCode, City as DeptCity, State as DeptState, AirlineCode, FlightNum, ArrAirportCode, sDeptDate, sDeptTime, sArrDate, sArrTime from
Airport join Flight 
on Airport.Code = Flight.DeptAirportCode)
join Airport on ArrAirportCode = Airport.Code
where Airport.City = 'Chicago' and Airport.State = 'Illinois'
) natural join DelayedFlight
where aDeptDate = '2012-01-03' and aArrDate = '2012-01-03'
)
) on H2ArrAirportCode = H3DeptAirportCode 
where julianday(H2aArrTime) < julianday(H3aDeptTime) and H3DeptAirportCode <> H1DeptAirportCode 
and H1DeptAirportCode <> H2DeptAirportCode and H2DeptAirportCode <> H3DeptAirportCode 
and H1ArrAirportCode <> H2ArrAirportCode and H2ArrAirportCode <> H3ArrAirportCode and H1ArrAirportCode <> H3ArrAirportCode
and julianday(H1aDeptTime) < julianday(H1aArrTime) and julianday(H2aDeptTime) < julianday(H2aArrTime) and julianday(H3aDeptTime) < julianday(H3aArrTime)
order by Duration asc

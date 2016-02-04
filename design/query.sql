 1. Get all airport codes in alphabetical order.
Input: N/A
Output: One column. airport ID.
A:
select Code from Airport 
order by Code ascd

2. Get all airline codes in alphabetical order.
Input: N/A
Output: One column. airline ID.
A:
select Code from Airline 
order by Code ascd

3.
Count the number of total flights.
Input: N/A
Output: One column. Number of flights.
A:
select count(*) from AllFlightRecord

4. Get all the reasons flights were delayed, along with their frequency, in order from
highest frequency to lowest.
Input: N/A
Output: Two columns. The first column should be a string describing the type of
delay. The four types of delays are Carrier Delay, Weather Delay, Air Traffic
Delay, and Security Delay. The second column should be the number of flights
that experienced that type of delay. The results should be in order from largest
number of flights to smallest.
A:
select Name, count(*) as count
from DelaySource 
group by Name
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

A: TBD: optimized this query, do where first and then join

select DeptAirportCode, ArrAirportCode, FlightSchedule.DeptDate + DeptTime
from FlightSchedule join AllFlightRecord on FlightSchedule.DeptDate = AllFlightRecord.DeptDate
where AirlineCode = ' ' and FlightNum = ' ' and DeptDate = ' '

6. Get all airlines, along with the number of flights by that airline which were scheduled
to depart on a particular day (whether or not they departed). Results should be
ordered from highest frequency to lowest frequency.
Input 1: A month (1 = January, 2 = February, ..., 12 = December)
Input 2: A day (1, 2 ... 31)
Input 3: A year (2010, 2011, 2012, etc)
Output: Two columns. The first column should be the name of the airline. The
second column should be the number of flights matching the criteria.

A:

select Name, Count(*) as CountNum
from 
(select Airline.Name,  FlightSchedule.ID 
from Airline join FlightSchedule 
on Airline.Code = FlightSchedule.AirlineCode)
join 
(select ID, DeptDate
from AllFlightRecord
where sDeptDate = ' ')
 on ?.ID = ?.ID
 group by Name ascd
 
7. For a specified set of airports, return the number of departing and the number of
arriving planes on a particular day (inclusive of delays). Results should be ordered
alphabetically by airport name, A-Z.
Input 1: A month (1 = January, 2 = February, ..., 12 = December)
Input 2: A day (1, 2 ... 31)
Input 3: A year (2010, 2011, 2012, etc)
Input 4 .. n: The full, canonical name of an airport (ie: LaGuardia).
Output: Three columns. The first column should be the name of the airport. The
second column should be the number of flights that departed the airport on the
specified day. The third column should be the number of flights that arrived at
the airport on the specified day.

A:
here use actual arrive dates.   
()
full join
()
































use database dcwt;
use schema schemaxx;
use warehouse query_wh;



// Previewing the data
/*
The first top 10 rows from the IP geolocation demo database.
*/
SELECT *
FROM ip_geolocation.demo.location
LIMIT 10;

// Let us write a quick Python UDF to convert IP addresses to corresponding integers
create or replace function dcwt.schemaxx.to_int(i string)
returns int
language python
runtime_version = '3.8'
handler = 'to_int_py'
as
$$

def to_int_py(i):
    parts = i.split('.')
    return ((int(parts[0]) << 24) + (int(parts[1]) << 16) + (int(parts[2]) << 8) + int(parts[3]))

 
$$;

// Let us test our function

select ipaddress, 
to_int(ipaddress) as ip_int
from dcwt.schemaxx.click_data
limit 10;


// Get Specific IP address data
/*
Use this query if you want to get geolocation information of a single IP address. Replace the IP address provided with your desired IP address.
*/
-- '24.183.120.0' ⇒ Input IP Address

SELECT *
FROM ip_geolocation.demo.location
WHERE TO_INT('24.183.120.0') 
BETWEEN start_ip_int AND end_ip_int;

-----------------
-- Explanation --
-----------------

-- TO_INT is a custom function that converts IP address values to their integer equivalent
-- start_ip_int represents the integer equivalent of the start_ip column
-- end_ip_int represents that integer equivalent of the end_ip column
-- The BETWEEN function checks to see if your input IP address falls between an the IP Range of start_ip_int and end_ip_int


// Top 10 Nearest IP Address from a location
/*
The Nearest IP address shows the closest IP addresses from a geographic coordinate. We use the “Haversine formula” to find IP addresses from the provided Latitude and Longitude values.
*/
-- 42.556 ⇒ Input Latitude
-- -87.8705 ⇒ Input Longitude

SELECT
  HAVERSINE(42.556, -87.8705, lat, lng) as distance,
  start_ip,
  end_ip,
  city,
  region,
  country,
  postal,
  timezone
FROM ip_geolocation.demo.location
order by 1 desc
limit 10;


-----------------
-- Explanation --
-----------------


-- Uses the Haversine Formula: https://en.wikipedia.org/wiki/Haversine_formula
-- The haversine formula determines the great-circle distance between two points on a sphere given their longitudes and latitudes.


--Get a count of pageviews and clicks aggregated by City and Region

--Get a count of pageviews and clicks aggregated by City and Region

select postal, city,timezone,count(*) as page_views, sum(click) as clicks from dcwt.schemaxx.click_data a
,IP_GEOLOCATION.DEMO.LOCATION b where substring(start_ip,1,10) = substring(ipaddress,1,10)
group by postal, city,timezone order by count(*) desc;







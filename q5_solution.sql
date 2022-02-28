/*
Q5) Imagine that new rental data is being loaded into the database every hour. 
--Assuming that the data is loaded sequentially, ordered by rental_date,
--re-purpose your logic for the customer_lifecycle table to process the new data in an incremental manner to a new table customer_lifecycle_incremental.
*/

-- Step 1: At the end of each run (before the next hourly data reaches the 'rental' table), we should insert the latest rental date from 
-- the 'rental' table which can be used in the next run as a condition to identify the new data.

Drop table if exists public.latest_rental_date;

Create table public.latest_rental_date (
    rental_date timestamp without time zone not null,
);

Insert into public.latest_rental_date
Select max(rental_date) latest_date
from public.rental
;

-- Step 2: Timeline wise, step 2 takes place after the new hourly data is loaded to the 'rental' table.
-- Create a 'staging_rental' table.
-- Locate the new data by selecting all rows with rental date larger than the existing latest rental date saved from the 'latest_rental_date' table.
-- Load them into the 'staging_rental' table.

Drop table if exists public.staging_rental;

Create table public.staging_rental (
    rental_id integer default nextval('public.rental_rental_id_seq'::regclass) not null primary key,
    rental_date timestamp without time zone not null,
    inventory_id integer not null,
    customer_id smallint not null,
    return_date timestamp without time zone,
    staff_id smallint not null,
    last_update timestamp without time zone default now() not null
);

Insert into public.staging_rental
Select * from public.rental 
where rental_date > (select max(latest_date) from public.latest_rental_date)
;

-- Step 3: Prepare the 'staging_rental' table. There are 2 scenarios here: if the new record is coming from an existing customer, 
-- it is necessary to load all previous rental records of this customer to the 'staging_rental' table because some of the lifecycle 
-- metrics (i.e. total revenue, latest film) require both the old + incremental (new) rental records for aggregation. 
-- For example -> customer 3 and customer 6 both have one new record. customer 3 is an existing customer while customer 6 is a new customer.
-- We need to bring in all previous records of customer 3 by doing a left join from 'staging_rental' to 'rental' on customer id.
-- As customer 6 is a new customer, coalesce() helps to retain the information from the left table after the left join as every column
-- from the right table will be NULLs. Afterwards, do a UNION between 'staging_rental' and desired 'staging_rental' to insert only the 
-- new rows into 'staging_rental'.

-- 'rental' 
-- | customer_id |      rental_date      |    ...   | 
-- |      3      |2006-02-14 15:16:03.000|old record|
-- |      3      |2006-02-14 15:17:03.000|old record|
-- |      4      |2006-02-14 15:30:03.000|old record|
-- |      5      |2006-02-14 16:14:03.000|old record|
-- |      3      |2006-02-14 16:18:34.000|new record|
-- |      6      |2006-02-14 16:30:44.000|new record|

-- 'staging_rental'
-- | customer_id |      rental_date      |    ...   |
-- |      3      |2006-02-14 16:18:34.000|new record|
-- |      6      |2006-02-14 16:30:44.000|new record|

-- Desired 'staging_rental'
-- | customer_id |      rental_date      |    ...   |
-- |      3      |2006-02-14 15:16:03.000|old record|
-- |      3      |2006-02-14 15:17:03.000|old record|
-- |      3      |2006-02-14 15:20:03.000|old record|
-- |      3      |2006-02-14 16:18:34.000|new record|
-- |      6      |2006-02-14 16:30:44.000|new record|

Insert into public.staging_rental
select * from public.staging_rental
union 
select 
	coalesce(r.customer_id, staging.customer_id) as customer_id,
	coalesce(r.rental_id, staging.rental_id) as rental_id,
	coalesce(r.rental_date, staging.rental_date) as rental_date,
	coalesce(r.inventory_id, staging.inventory_id) inventory_id,
	coalesce(r.return_date, staging.return_date) return_date,
	coalesce(r.staff_id, staging.staff_id) staff_id,
	coalesce(r.last_update, staging.last_update) last_update
	from public.staging_rental staging
left join public.rental r using (customer_id)
;

-- Step 4: When the 'staging_rental' table is prepared, we create a 'customer_life_cycle_incremental' table where we load the 
-- output of the customer lifecycle scripts (from Q4) after running them on the 'staging_rental table' (Please note that the scripts in 
-- Q4 are run on all unique customers from 'rental', but what is suggested in this step is to run the Q4 scripts on 'staging_rental', 
-- meaning we are only processing the incremental rows that are added every hour). The desired output of the 'customer_lifecycle_incremental' 
-- table should look like this: 

-- 'customer_lifecycle_incremental'
-- | customer_id |...|   latest_rent_date    |
-- |      3      |...|2006-02-14 16:18:34.000|
-- |      6      |...|2006-02-14 16:30:44.000|

Drop table if exists public.customer_lifecycle_incremental;

Create table public.customer_lifecycle_incremental (
	customer_id integer not null primary key,
	first_rent_date timestamp not null,
	thirty_days_date timestamp not null,
	thirty_days_revenue numeric(10,5) not null,
	value_tier integer not null,
	first_film varchar(255) not null,
	latest_film varchar(255) not null,
	last_rental_date timestamp not null,
	avg_time_days_between_rental numeric(5,2) not null,
	total_revenue numeric(10,5) not null,
	fav_actor_one varchar(50) not null,
	fav_actor_two varchar(50) not null,
	fav_actor_three varchar(50) not null,
	revenue_growth_rate numeric(10,5) not null
);

-- Here, we largely follow the same process laid out in Q4 where we create CTE tables for lifecycle metrics, but instead of selecting
-- unique users from 'rental' as the source table in the left joins, we select from 'staging_rental' to process only the incremental rows.
-- All CTE tables querying from 'rental' should be changed to 'staging_rental' as the goal here is to generate an *updated* customer lifecycle 
-- row (1 row per customer) with the inclusion of the new data. The skeleton of the CTE tables in Q4 should look like the following:
-- (CTE codes commented out to avoid re-writing the full section in Q4)

-- with total_revenue as (
-- 	select r.customer_id,
--		sum(coalesce(p.amount,0)) as amount,
--		count(rental_id) as num_of_rentals
--	from public.staging_rental r 
--	left join public.payment p using (rental_id)
--	group by 1),
-- ...
-- revenue_growth_rate as (
-- ...)

Insert into public.customer_lifecycle_incremental
select
	sr.customer_id,
	tdd.first_rent_date, 
	tdd.thirty_days_date,
	tdr.amount,
	tdr.value_tier,
	flf.first_film,
	flf.latest_film,
	flf.last_rental_date,
	art.avg_rental_time,
	tr.amount,
	fta.fav_actor_one,
	fta.fav_actor_two,
	fta.fav_actor_three,
	rgr.rate_of_revenue_growth
from (select distinct customer_id from public.staging_rental) sr 

left join total_revenue tr on sr.customer_id = tr.customer_id
left join thirty_days_date tdd on sr.customer_id = tdd.customer_id
left join thirty_days_revenue tdr on sr.customer_id = tdr.customer_id
left join first_latest_film flf on sr.customer_id = flf.customer_id
left join avg_rental_time art on sr.customer_id = art.customer_id 
left join fav_three_actors fta on sr.customer_id = fta.customer_id 
left join revenue_growth_rate rgr on sr.customer_id = rgr.customer_id
;

-- Step 5: 
-- Insert the *updated* customer lifecycle row into the 'customer_lifecycle' table.
-- For instance, customer 3 will have 2 rows at this stage (old and new) in the 'customer_lifecycle' table.
-- Delete the old record from the 'customer_lifecycle' table.
-- Now, the 'customer_lifecycle' table will have an *updated* record for every existing customer and a new record for every new customer.

-- new *updated* 'customer_lifecycle'
-- | customer_id |...|   latest_rent_date    |
-- |      3      |...|2006-02-14 16:18:34.000|
-- |      4      |...|2006-02-14 15:30:03.000|
-- |      5      |...|2006-02-14 16:14:03.000|
-- |      6      |...|2006-02-14 16:30:44.000| 

Insert into public.customer_lifecycle
Select * from public.customer_lifecycle_incremental
;

Delete from public.customer_lifecycle t1
using public.customer_lifecycle_incremental t2
where t1.customer_id = t2.customer_id
and t1.rental_date < t2.rental_date
;
 

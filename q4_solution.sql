/*
Q4) Create a table, customer_lifecycle, with a primary key of customer_id. Please include all the required DDL. 
This table is designed to provide a holistic view of a customers activity and should include:

--The revenue generated in the first 30 days of the customer's life-cycle, with day 0 being their first rental date.
--A value tier based on the first 30 day revenue.
--The name of the first film they rented.
--The name of the last film they rented.
--Last rental date.
--Avg. time between rentals.
--Total revenue.
--The top 3 favorite actors per customer.
--Any other interesting dimensions or facts you might want to include.

Explanation:
1) In this 'customer_lifecycle' table, each customer should have only 1 row of data as customer id is set as primary key 
(599 unique customers -> 599 rows). 

2) For each group of metrics asked in the question, I saved them in seperate CTE tables for ease of logic tracking and 
left joined each CTE to a distinct list of customers queried from the central 'rental' table after the INSERT statement
in line 182.

3) I noticed that there are 71 customers (i.e. customer 61, customer191) whose first 30 days rentals' payment cannot be 
referenced from the 'payment' table by joining on rental id. I set these customers' first 30-day revenue as zero.

4) Another thing I noticed with the rental date is that there are rental records from May 2005 - August 2005, but there 
are no rental records in subsequent months until Feb 2006. This should be investigated further in a real-life business setting
as it could suggest missing data.

5a) As for the interesting dimension, I decided to look at the change in revenue generated per day by each customer between 
the 30th day and the latest rental day, and compared it to the first 30 days'. Customers with higher revenue generated per day 
after the first 30 days (with positive % increase) could mean they are more likely to be loyal/long-term customers. 
Customers with negative % increase on revenue generated per day after the first 30 days could mean they are disengaged
and the company should think of ways to incentivize these customers, perhaps via targeted movie recommendations. 

5b) For example, customer 29 spent £21.93 on rentals in the first 30 days, with a revenue of $0.731/day. Between the 30th day 
and the last rental day (Feb 14 2006), customer 29 spent £116.72 (£138.65 - £21.93), giving a revenue of £0.498/day and 
a 31.87% decrease in revenue generated per day after the first 30 days.
*/

Drop table if exists public.customer_lifecycle;

Create table public.customer_lifecycle (
	customer_id int2 not null primary key,
	first_rent_date timestamp not null,
	thirty_days_date timestamp not null,
	thirty_days_revenue numeric(10,5) not null,
	value_tier int2 not null,
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

with total_revenue as ( -- total revenue between the last rental date and the first rental date from each customer
	select r.customer_id,
		sum(coalesce(p.amount,0)) as amount,
		count(rental_id) as num_of_rentals
	from public.rental r 
	left join public.payment p using (rental_id)
	group by 1
),

fav_three_actors as ( -- top three favourite actors from each customer based on the actors' number of appearances in the films they rented
	select customer_id, 
			max(case when row_num = 1 then actor_name else null end) as fav_actor_one,
			max(case when row_num = 2 then actor_name else null end) as fav_actor_two,
			max(case when row_num = 3 then actor_name else null end) as fav_actor_three
	from (
		select customer_id, actor_name, row_num from 
			(select customer_id, 
				actor_name,
				count(rental_id) as num_count, 
				row_number() over (partition by customer_id order by count(rental_id) desc, actor_name) as row_num 
			from (select 
					r.customer_id, 
					r.rental_id,
					a.first_name || ' ' || a.last_name as actor_name
				  from public.rental r
				  left join inventory i using (inventory_id)
				  left join film f using (film_id)
				  left join film_actor fa using (film_id)
				  left join actor a using (actor_id)
				 ) actors_list
			group by customer_id, actor_name ) rank_actors
		where row_num < 4 ) flat_row
	group by 1
),

first_latest_film as ( -- obtain the first film name and latest film name each customer rented. 
	select * from ( 
		select  -- move latest film info up by 1 row to achieve flat table structure, each customer with 1 data row
			first_last.customer_id,
			first_last.inventory_id,
			first_last.rental_date as first_rental_date,
			f.title as first_film,
			lead(inventory_id) over (partition by customer_id order by rental_date) as last_film_inventory_id,
			lead(rental_date) over (partition by customer_id order by rental_date) as last_rental_date,
			lead(title) over (partition by customer_id order by rental_date) as latest_film
			
		from ( 	-- all customers will only have 2 data rows, first and last rental date. 
			select 
				dedup.customer_id,
				dedup.inventory_id,
				dedup.rental_date, 
				row_num,
				min(row_num) over (partition by dedup.customer_id) as first_rental, 
				max(row_num) over (partition by dedup.customer_id) as last_rental 
			from (
				select record.* from ( -- customers can have more than 2 data rows (due to min/max rental records with same timestamp). If so, they are ranked by rental id
					select r.customer_id,
						r.inventory_id,
						r.rental_date,
						row_number() over (partition by rent.customer_id order by rental_id) row_num 
					from public.rental r
					inner join (
						select distinct customer_id, -- get min and max rental date for each customer
							min(rental_date) over(partition by customer_id order by customer_id) first_rent_date,
							max(rental_date) over(partition by customer_id order by customer_id) latest_rent_date 
						from public.rental ) rent
					on (r.customer_id = rent.customer_id) and (r.rental_date = rent.first_rent_date or r.rental_date = rent.latest_rent_date) 
				where rent.customer_id is not null ) record
				) dedup
			) first_last
		left join public.inventory i using (inventory_id)
		left join public.film f using (film_id)
		where row_num = first_rental or row_num = last_rental 
		) flat_table
	where latest_film is not null
),

thirty_days_date as ( -- 1st and 30th day of each customer
	select customer_id, 
		min(rental_date) first_rent_date, 
		min(rental_date + interval '30 DAY') thirty_days_date
	from public.rental 
	group by 1
),

thirty_days_revenue as ( -- revenue generated by each customer in the first 30 days
	select customer_id, 
		sum(coalesce(amount,0.0)) as amount,
		ntile(10) over (order by sum(coalesce(amount,0.0)) desc) as value_tier
	from (
		select r.customer_id, rental_date, p.amount
		from public.rental r
		left join payment p using (rental_id)
		left join thirty_days_date tdd on r.customer_id = tdd.customer_id
		where rental_date >= tdd.first_rent_date and r.rental_Date < tdd.thirty_days_date ) thirty
	group by 1
),

avg_rental_time as ( -- average rental time is defined as the day difference between the first and last rental date, divided by total number of rental records
	select customer_id, 
	extract(epoch from last_rental_date - first_rental_date) / 86400 / num_of_rentals as avg_rental_time
	from total_revenue 
	left join thirty_days_date using(customer_id)
	left join first_latest_film using(customer_id)
),

revenue_growth_rate as ( -- compare revenue generated per day in the first 30 days v.s. revenue generated per day between 30th day and latest rental day, compute the percentage change of the two
	select tdr.customer_id,
		case 
			when tdr.amount = 0 then 
				round((((tr.amount - tdr.amount) / 
				(extract(epoch from flf.last_rental_date - tdd.thirty_days_date) / 86400)) - (tdr.amount / 30)) * 100 , 2) 
			else
				round((((tr.amount - tdr.amount) / 
				(extract(epoch from flf.last_rental_date - tdd.thirty_days_date) / 86400)) - (tdr.amount / 30)) / (tdr.amount / 30) * 100, 2)
		end as rate_of_revenue_growth
	from thirty_days_revenue tdr 
	left join first_latest_film flf using(customer_id)
	left join thirty_days_date tdd using(customer_id)
	left join total_revenue tr using(customer_id)
)

insert into public.customer_lifecycle
select
	r.customer_id,
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
from (select distinct customer_id from public.rental) r 

left join total_revenue tr on r.customer_id = tr.customer_id
left join thirty_days_date tdd on r.customer_id = tdd.customer_id
left join thirty_days_revenue tdr on r.customer_id = tdr.customer_id
left join first_latest_film flf on r.customer_id = flf.customer_id
left join avg_rental_time art on r.customer_id = art.customer_id 
left join fav_three_actors fta on r.customer_id = fta.customer_id 
left join revenue_growth_rate rgr on r.customer_id = rgr.customer_id
;


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
1) In this 'customer_lifecycle' table, each customer should have only 1 row of data as customer id is set as primary key. 

2) For each group of metrics asked as requirements, I saved them in seperate cte tables and left joined each cte to a distinct list of users obtained 
from the central 'rental' table for ease of logic tracking.

3) I noticed that there are 71 customers (i.e. customer 61,191) whose first 30 days rentals' payment cannot be referenced from the 'payment' table 
by joining on rental ids. Hence, I set their first 30 day revenue to zero.

4) Another thing I noticed with the rental date, there are rental records from May-August 2005, then it jumps straight to Feb 2006. This should be 
investigated further in a real life setting, which could suggest missing data.

5) As for the interesting dimension, I decided to look at the change in revenue generated on each customer between the 30th day to latest rental day.
Customers with higher revenue generated per day outside of the first 30 days (positive % increase) could mean they are more likely to be 
loyal/long term customers. Customers with slowing negative % increase on revenue generated could mean they are less willing to spend money in the 
long run and the company should think of ways to retain these customers, perhaps via targeted movie recommendation. 
For example, customer 29 spent an $21.93 in the first 30 days, which gives a revenue of $0.731/day. Between 30th day and last rental day (Feb 14 2006), 
the customer spent $116.72 ($138.65 - $21.93) and a revenue of $0.498 per day. Hence, there is a 31.87% drop in revenue from customer 29 per day 
after the first 30 days.
*/

Drop table if exists customer_lifecycle;

Create table customer_lifecycle (
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

with total_revenue as ( --total revenue between the last rental date and the first rental date from each customer
	select r.customer_id, sum(coalesce(p.amount,0)) as amount, count(rental_id) as num_of_rentals
	from public.rental r 
	left join public.payment p using (rental_id)
	group by 1
),

fav_three_actors as ( --top three favourite actors from each customer, based on the actors' number of appearance in the films the customer rented
	select customer_id, 
			max(case when row_num = 1 then actor_name else null end) as fav_actor_one,
			max(case when row_num = 2 then actor_name else null end) as fav_actor_two,
			max(case when row_num = 3 then actor_name else null end) as fav_actor_three
	from (
		select customer_id, actor_name, row_num from 
			(select customer_id, actor_name, count(rental_id) as num_count, 
			row_number() over (partition by customer_id order by count(rental_id) desc, actor_name) as row_num 
			from (select *, a.first_name || ' ' || a.last_name as actor_name
				  from public.rental
				  left join inventory i using (inventory_id)
				  left join film f using (film_id)
				  left join film_actor fa using (film_id)
				  left join actor a using (actor_id)
				 ) actors_list
			group by customer_id, actor_name) rank_actors
		where row_num < 4 ) flat_row
	group by customer_id
),

first_latest_film as ( --to obtain the first film name and latest film name each customer rented
	select * from ( 
		select  --move latest film info up by 1 row to achieve flat structure, each customer with 1 line
			first_last.customer_id,
			first_last.inventory_id as first_film_inventory_id,
			first_last.rental_date as first_rental_date,
			f.title as first_film,
			lead(inventory_id) over (partition by customer_id order by rental_date) as last_film_inventory_id,
			lead(rental_date) over (partition by customer_id order by rental_date) as last_rental_date,
			lead(title) over (partition by customer_id order by rental_date) as latest_film
			
		from ( 	--to ensure each customer only has 2 data rows, 1 for max and 1 for min
			select dedup.customer_id, dedup.inventory_id, dedup.rental_date, row_num,
				min(row_num) over (partition by dedup.customer_id) as first_rental, 
				max(row_num) over (partition by dedup.customer_id) as last_rental 
			from (
				select record.* from ( --there can be min/max rentals with same timestamp, rank by rental id
					select r.*,
						row_number() over (partition by rent.customer_id order by rental_id) row_num 
					from public.rental r
					inner join (
						select distinct customer_id, --get min and max date for each customer
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

thirty_days_date as ( --the 1st and 30th day of each customer
	select customer_id, 
		min(rental_date) first_rent_date, 
		min(rental_date + interval '30 DAY') thirty_days_date
	from public.rental 
	group by 1
),

thirty_days_revenue as ( --revenue generated in the first 30 days of each customer
	select customer_id, 
		sum(coalesce(amount,0)) as amount,
		ntile(10) over (order by sum(coalesce(amount,0)) desc) as value_tier
	from (
		select r.customer_id, rental_date, p.amount
		from public.rental r
		left join payment p using (rental_id)
		left join thirty_days_date tdd on r.customer_id = tdd.customer_id
		where rental_date >= tdd.first_rent_date and r.rental_Date < tdd.thirty_days_date ) thirty
	group by 1
),

avg_rental_time as ( --average rental time is defined as the day difference between first and last rental date, divided by total number of rentals
	select customer_id, 
	extract(epoch from last_rental_date - first_rental_date) / 86400 / num_of_rentals as avg_rental_time
	from total_revenue 
	left join thirty_days_date using(customer_id)
	left join first_latest_film using(customer_id)
),

revenue_growth_rate as ( --comparing revenue generated per day in the first 30 days v.s. revenue generated per day between 30th day and latest rental day, compute the percentage change of the two
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

insert into customer_lifecycle
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


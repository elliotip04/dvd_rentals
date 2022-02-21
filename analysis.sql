--Q1) Find the top 10 most popular movies from rentals in H1 2005, by category.

with fav_movie as (
	select 
		c.name as category_name,
		f.title,
		count(r.customer_id) as num_of_rental,
		row_number() over (partition by c.name order by count(r.customer_id) desc) as row_num
	from rental r
	
	left join inventory i using(inventory_id)
	left join film f using (film_id)
	left join film_category fc using (film_id) 
	left join category c using (category_id)
	where date(rental_date) between '2005-01-01' and '2005-06-30'
	group by 1,2
	)
select category_name, title, num_of_rental 
from fav_movie
where row_num <= 10
;

--Q2) Find the avg. customer value per store by month for rentals in 2005.
--Please exclude the top & bottom 10% of customers by value from the analysis.

with cust_mid_revenue as --remove top & bottom 10% of customers by revenue in 2005
	(select * from (
		select r.customer_id, sum(p.amount) as amount, ntile(100) over (order by sum(amount)) as rank
		from rental r 
		left join payment p using (rental_id)
		where extract(year from date(rental_date)) = '2005' and p.amount is not null --not paid yet? 1452 rentals with no payment amounts 
		group by 1) percentile
	where rank between 11 and 90) --the bins begin at 1, end at 100

select 
	to_char(date(rental_date),'YYYY-MM') as month,
	i.store_id,
	avg(p.amount) as amount
from rental 
inner join cust_mid_revenue using (customer_id) --retain only the rental records of middle 80% of customers 
left join payment p using (rental_id)
left join inventory i using (inventory_id)
where extract(year from date(rental_date)) = '2005' and p.amount is not null
group by 1,2
order by 1,2,3 desc
;

--Q3) Create a table, film_recommendations, which provides 10 film recommendations per customer. Future recommendations could be based upon a customer's previous film choices, other customer's choices etc.
-- Please only use SQL to complete this and include all the DDL needed to create the table.
 
Drop table if exists film_recommendations;

Create table film_recommendations (
	customer_id int2 not null,
	customer_name varchar(50) not null,
	category varchar(25) not null,
	title varchar(255) not null,
	film_id int4 not null
);

with rental_history as (
	select distinct r.customer_id,
		cu.first_name || ' ' || cu.last_name as full_name, 
		f.title, 
		c.name,
		count(*) over (partition by customer_id, name order by name) as num_rental
	from rental r
	
	left join customer cu using(customer_id)
	left join inventory i using (inventory_id)
	left join film f using (film_id)
	left join film_category fc using (film_id) 
	left join category c using (category_id)
	),

fav_category as (
	select * 
	from rental_history 
	inner join 
		(select customer_id, full_name, max(num_rental) as num_rental
		from rental_history 
		group by customer_id, full_name
		) num
	using (full_name, num_rental)
	),
	
movie_list as (
	select film_id, name, title, rental_rate, rental_duration
	from category
	left join film_category using (category_id)
	left join film using (film_id)
),

movies_not_watched_each_customer as (
	select all_movie.*, renters.*
	from (select * from movie_list) all_movie
	
	cross join (Select distinct customer_id, full_name from rental_history) renters
	left join rental_history fc using (full_name, title)
	where fc.name is null
),

movie_recommend as (
	select customer_id, m.full_name, m.name, title, film_id, rental_rate, rental_duration,
	row_number() over (partition by m.full_name order by rental_rate desc, rental_duration asc) as row_num 
	from movies_not_watched_each_customer m
	inner join (select distinct full_name, name from fav_category) fav using (full_name, name)
)

insert into film_recommendations
select customer_id, full_name, name, title, film_id
from movie_recommend
where row_num <= 10
;

--Q4) Create a table, customer_lifecycle, with a primary key of customer_id. Please include all the required DDL. This table is designed to provide a holistic view of a customers activity and should include:

--The revenue generated in the first 30 days of the customer's life-cycle, with day 0 being their first rental date.
--A value tier based on the first 30 day revenue.
--The name of the first film they rented.
--The name of the last film they rented.
--Last rental date.
--Avg. time between rentals.
--Total revenue.
--The top 3 favorite actors per customer.
--Any other interesting dimensions or facts you might want to include.

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
	fav_actor_one_in_recommendation int2 not null,
	fav_actor_two_in_recommendation int2 not null,
	fav_actor_three_in_recommendation int2 not null,
	recommendation_score numeric(10,5) not null
);

with total_revenue as (
	select r.customer_id, sum(coalesce(p.amount,0)) as amount
	from rental r 
	left join payment p using (rental_id)
	group by 1
),

fav_three_actors as (
	select customer_id, 
			max(case when row_num = 1 then actor_name else null end) as fav_actor_one,
			max(case when row_num = 2 then actor_name else null end) as fav_actor_two,
			max(case when row_num = 3 then actor_name else null end) as fav_actor_three
	from (
		select customer_id, actor_name, row_num from 
			(select customer_id, actor_name, count(rental_id) as num_count, 
			row_number() over (partition by customer_id order by count(rental_id) desc, actor_name) as row_num 
			from (select *, a.first_name || ' ' || a.last_name as actor_name
				  from rental
				  left join inventory i using (inventory_id)
				  left join film f using (film_id)
				  left join film_actor fa using (film_id)
				  left join actor a using (actor_id)
				 ) actors_list
			group by customer_id, actor_name) rank_actors
		where row_num < 4 ) flat_row
	group by customer_id
),

thirty_days_detail as (
	select * from (
		select r.customer_id, thirty.first_rent_date, thirty.thirty_days_date, 
			f.title as first_film, 
			lead(f.title, 1) over (partition by r.customer_id order by rental_date asc) as latest_film,
			lead(r.rental_date, 1) over (partition by r.customer_id order by rental_date asc) as last_rental_date
		from rental r
		inner join (
			select customer_id, 
				min(rental_date) first_rent_date, 
				min(rental_date + interval '30 DAY') thirty_days_date
			from rental 
			group by customer_id) thirty
		
		on (r.customer_id = thirty.customer_id) and 
			(r.rental_date = thirty.first_rent_date or r.rental_date = (select rental_date from rental r 
				where r.customer_id = thirty.customer_id and r.rental_date <= thirty.thirty_days_date 
				order by rental_date desc limit 1) )
			
		left join inventory i using (inventory_id)
		left join film f using (film_id)
		) mapp
	where latest_film is not null
),

recommend as (
	select cl.customer_id, title, film_id, actor_name
	from customer_lifecycle cl
	
	left join( select fr.*, a.first_name || ' ' || a.last_name as actor_name
		from film_recommendations fr
		left join film_actor using (film_id) 
		left join actor a using (actor_id)
		) film_actors
	on (cl.customer_id = film_actors.customer_id) and 
		(cl.fav_actor_one = film_actors.actor_name or cl.fav_actor_two = film_actors.actor_name or 
		cl.fav_actor_three = film_actors.actor_name)
	where title is not null
),

recommendation_score as (
	select *, 
		(fav_actor_one_in_recommendation + fav_actor_two_in_recommendation + fav_actor_three_in_recommendation) / 3.0 as recommendation_score
	from (
		select *,
			case when customer_id in (select customer_id from recommend) and fav_actor_one in (select actor_name from recommend)
			then 1 else 0 end as fav_actor_one_in_recommendation,
			case when customer_id in (select customer_id from recommend) and fav_actor_two in (select actor_name from recommend)
			then 1 else 0 end as fav_actor_two_in_recommendation,
			case when customer_id in (select customer_id from recommend) and fav_actor_three in (select actor_name from recommend)
			then 1 else 0 end as fav_actor_three_in_recommendation
		from fav_three_actors
	) score
),

combined as (
	select
		r.customer_id,
		tdd.first_rent_date, 
		tdd.thirty_days_date,
		sum(coalesce(p.amount,0)) thirty_days_revenue,
		ntile(10) over (order by sum(coalesce(p.amount,0)) desc) value_tier,
		tdd.first_film,
		tdd.latest_film,
		tdd.last_rental_date,
		extract(epoch from tdd.last_rental_date - tdd.first_rent_date) / 86400 / count(*),
		tr.amount,
		fta.fav_actor_one,
		fta.fav_actor_two,
		fta.fav_actor_three,
		rs.fav_actor_one_in_recommendation,
		rs.fav_actor_two_in_recommendation,
		rs.fav_actor_three_in_recommendation,
		rs.recommendation_score
	from rental r 
	
	left join payment p using (rental_id)
	left join total_revenue tr on r.customer_id = tr.customer_id
	left join fav_three_actors fta on r.customer_id = fta.customer_id 
	left join thirty_days_detail tdd on r.customer_id = tdd.customer_id
	left join recommendation_score rs on r.customer_id = rs.customer_id
	
	where rental_date >= first_rent_date and rental_Date < thirty_days_date
	group by 1,2,3,6,7,8,10,11,12,13,14,15,16,17
)

insert into customer_lifecycle
select * from combined

select * from customer_lifecycle

--Q5) Imagine that new rental data is being loaded into the database every hour. 
--Assuming that the data is loaded sequentially, ordered by rental_date,
--re-purpose your logic for the customer_lifecycle table to process the new data in an incremental manner 
--to a new table customer_lifecycle_incremental.


1) drop the historic table
2) create a new table 

defein a trigger when a new only 
define a trigger the rental table, when newtable comes in, we drop 

appraoch: create this as a view (how much , depends on how often you will revisit this table)

create a view
howver cons of a view is: when data is large


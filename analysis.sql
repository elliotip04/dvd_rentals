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
select category_name, title, number_of_rental 
from fav_movie
where row_num <= 10

--Q2) Find the avg. customer value per store by month for rentals in 2005.
--Please exclude the top & bottom 10% of customers by value from the analysis.

with customers_mid_revenue as --remove top & bottom 10% of customers by revenue in 2005
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
inner join customers_mid_revenue using (customer_id) --retain only the rental records of middle 80% of customers 
left join payment p using (rental_id)
left join inventory i using (inventory_id)
where extract(year from date(rental_date)) = '2005' and p.amount is not null
group by 1,2
order by 1,2,3 desc

--Q3) Create a table, film_recommendations, which provides 10 film recommendations per customer. Future recommendations could be based upon a customer's previous film choices, other customer's choices etc.
-- Please only use SQL to complete this and include all the DDL needed to create the table.
	drop table if exists 
	create table (
	)
/*	cte  with as ....
	insert into 
	select from */
 
with rental_history as (
	select 
		distinct cu.first_name || ' ' || cu.last_name as full_name, 
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
		(select full_name, max(num_rental) as num_rental
		from rental_history 
		group by full_name
		) num
	using (full_name, num_rental)
	),
	
movie_list as (
	select name, title, rental_rate, rental_duration
	from category
	left join film_category using (category_id)
	left join film using (film_id)
),

movies_not_watched_each_customer as (
	select all_movie.*, renters.*
	from (select * from movie_list) all_movie
	
	cross join (Select distinct full_name from rental_history) renters
	left join rental_history fc using (full_name, title)
	where fc.name is null
),

movie_recommend as (
	select m.full_name, m.name, title, rental_rate, rental_duration,
	row_number() over (partition by m.full_name, m.name order by rental_rate desc, rental_duration asc) as row_num 
	from movies_not_watched_each_customer m
	inner join (select distinct full_name, name from fav_category) fav using (full_name, name)
)

select * from movie_recommend
where row_num <= 10


--Q4) Create a table, customer_lifecycle, with a primary key of customer_id. Please include all the required DDL. This table is designed to provide a holistic view of a customers activity and should include:

--The revenue generated in the first 30 days of the customer's life-cycle, with day 0 being their first rental date.
--A value tier based on the first 30 day revenue.
--The name of the first film they rented.
--The name of the last film they rented.
--Last rental date.
--Avg. time between rentals.
--Total revenue.
--The top 3 favorite actors per customer. (Can store in a different table)
--Any other interesting dimensions or facts you might want to include.

customer id | actor 1 | actor 2| actor 3 | count 

customer id, actor, count(), row_number 1,2,3,4,5

group by customer id, max(case when row_number = 1, actor else null), max(when row_number = 2)...3, 

select
	r.customer_id, 
	r.rental_date,
	p.amount
	--thirty.first_rent_date, 
	--thirty.thirty_rent_date
from rental r 

left join payment p using (rental_id)
left join 
	(select customer_id, min(rental_date) first_rent_date, min(rental_date) + interval '30 DAY' thirty_rent_date
	from rental 
	group by customer_id) thirty using (customer_id)



where p.amount is not null --not paid yet? 1452 rentals with no payment amounts

appraoch: create this as a view (how much , depends on how often you will revisit this table)




--Q5) Imagine that new rental data is being loaded into the database every hour. Assuming that the data is loaded sequentially, ordered by rental_date,
-- re-purpose your logic for the customer_lifecycle table to process the new data in an incremental manner to a new table customer_lifecycle_incremental.


1) drop the historic table
2) create a new table 

defein a trigger when a new only 
define a trigger the rental table, when newtable comes in, we drop 

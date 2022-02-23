/* 
Q3) Create a table, film_recommendations, which provides 10 film recommendations per customer.
Future recommendations could be based upon a customer's previous film choices, other customer's choices etc.
Please only use SQL to complete this and include all the DDL needed to create the table.

Explanation:
- My desired output for the 'film_recommendations' table is to have 10 rows for each customer. Each row should have one 
recommended movie and category (599 unique customers x 10 movies recommended = 5990 rows).  

- On a hight level, my movie recommendations are based on customers' previous film choices. For each customer, I looked at 
which film category they have made the most rentals from, with the assumption that the movie categories which they rented 
the most from are their favourite movie categories. 

- From these categories, I recommended movies which they have not rented before and ranked these movies based on rental rate 
and rental duration. Movies with higher rental rate (better profit margin) and shorter rental duration (quicker turnaround) 
are ranked higher. Subsequently, each customer is recommended the top 10 ranked movies.

Description of each CTE table:
1) rental_history 
This table shows every movie each customer rented, and the total number of rentals they made within the same movie category.
(For example, customer 31 (Brenda Wright) rented 4 movies from the 'Classics' category, which is reflected by 4 separate 
data rows in this CTE table with the same customer name, 4 different movie titles, same movie category, and same total 
number of rentals in that particular movie category).

2) fav_category
This table is a filtered version of 'rental_history' with only the rental records from each customer's favourite movie category,
which is defined by having the most number of rentals. In the case where a customer rented the same number of movies from multiple
movie categories, the rental records of these movie categories will all be preserved in this CTE table.

3) movie_list 
The full list of movies rented.

4) movies_not_watched_each_customer
For each customer, I created a matrix which cross-joined the full 'movie_list' and 'rental_history', and subsequently left joined 
with 'fav_category'. Movies that have not been rented by each customer within their favourite movie category/categories will be 
shown as NULL values and kept in this CTE table.

5) movie_recommend
Records in 'movies_not_watched_each_customer' are ranked by rental rate and rental duration, with higher rental rate and lower 
rental duration ranked higher.
**/

Drop table if exists public.film_recommendations;

Create table public.film_recommendations (
	customer_id integer not null,
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
	from public.rental r
	
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
	select film_id,
		name, 
		title, 
		rental_rate,
		rental_duration
	from public.category
	left join film_category using (category_id)
	left join film using (film_id)
),

movies_not_watched_each_customer as (
	select 
		all_movie.*, 
		renters.*
	from (select * from movie_list) all_movie
	cross join (Select distinct customer_id, full_name from rental_history) renters
	left join rental_history fc using (full_name, title)
	where fc.name is null
),

movie_recommend as (
	select 
		customer_id,
		m.full_name,
		m.name,
		title,
		film_id,
		rental_rate,
		rental_duration,
		row_number() over (partition by m.full_name order by rental_rate desc, rental_duration asc) as row_num 
	from movies_not_watched_each_customer m
	inner join (select distinct full_name, name from fav_category) fav using (full_name, name)
)

insert into public.film_recommendations
select customer_id,full_name, name, title, film_id
from movie_recommend
where row_num <= 10
;

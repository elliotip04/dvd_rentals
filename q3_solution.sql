
/* 
Q3) Create a table, film_recommendations, which provides 10 film recommendations per customer. Future recommendations could be based upon a customer's previous film choices, other customer's choices etc.
Please only use SQL to complete this and include all the DDL needed to create the table.

Explanation:
My desired output for the 'film_recommendations' table is there are 10 rows for each customer. Each row should have one recommended movie and category.
On a hight level, my movie recommendations are based on a customer's previous film choice. For each customer, I looked at which film category they have
made the most rentals from, with the assumption that the movie categores which they have most rented from are their favouriate categories. 
From their favourite categories, I recommended movies which they have not rented before and ranked these movies based on rental rate and rental duration.
Movies with higher rental rate (better margin) and showered rental duration (quicker turnaround ) are ranked higher up and the top 10 ranked movies
are recommended to each customer.

Description of each cte:
1) rental_history - all movies each customers watched and the number of rentals they made in the same movie cataegory of the same category they watched
 (for example, customer 31 Brenda Wright has rented 4 movies from the 'Classics' category, which will be reflected by 4 different data rows in this cte
 with the same customer name, 4 different movie titles, same movie category, and same number of rentals)

2) fav_category - this table is a filtered version of 'rental_history' with only the rows from each customer's favourite movie category, which is defined
by having the most number of rentals. In the case where a customer rented the same number of movies from different movie categories, the data rows
from these movie categories will all be preserved.

3) movie_list - full list of movies rented

4) movies_not_watched_each_customer - for each customer,I created a matrix which cross-joined the full movie_list and rental_history, and further 
left joined with fav_category. Movies that have not been rented by each customer from their favourite category will be NULL values and kept in this cte.

5) movie_recommend - records in 'movies_not_watched_each_customer' were ranked by rental rate and rental duration, with higher rental rate and lower 
rental duration ranked higher up.
**/

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
	select film_id, name, title, rental_rate, rental_duration
	from public.category
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

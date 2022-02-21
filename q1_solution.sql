--Q1) Find the top 10 most popular movies from rentals in H1 2005, by category.

with fav_movie as (
	select 
		c.name as category_name,
		f.title,
		count(r.customer_id) as num_of_rental,
		row_number() over (partition by c.name order by count(r.customer_id) desc) as row_num
	from public.rental r
	
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
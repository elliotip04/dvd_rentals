--Q2) Find the avg. customer value per store by month for rentals in 2005.
--Please exclude the top & bottom 10% of customers by value from the analysis.

/* Explanation: 
1) The question did not specify whether to exclude top & bottom 10% of customers from each store or from an overall basis,
Therefore, my calculations take the assumption of excluding customers based on their total revenue in 2005, regardless of stores.

2) May 2005 shows zero amount in average customer value per store as its customers are excluded. Line 19 chose rank between 2 and 9 as 
ntile() bins are inclusive of 2 and 9 (and exclude 1 and 10)

3) I also noticed that there are 1452 rental records (71 unique customers) in the 'rental' table which their payment amounts cannot be referenced from the 'payment' table,
hence I treated them as zero payments. My approach to this question is to create a CTE with the full list of the middle 80% of customers, 
then inner join with the main 'rental' table and average the payment amount by month in 2005. 
*/

with cust_mid_revenue as 
	(select * from (
		select r.customer_id, sum(coalesce(p.amount, 0.0)) as amount,
		ntile(10) over (order by sum(coalesce(p.amount, 0.0))) as rank
		from public.rental r 
		left join public.payment p using (rental_id)
		where extract(year from date(rental_date)) = '2005' 
		group by 1) percentile
	where rank between 2 and 9) 

select 
	to_char(date(rental_date),'YYYY-MM') as month,
	i.store_id,
	avg(coalesce(p.amount,0.0)) as amount
from public.rental 
inner join cust_mid_revenue using (customer_id) --retain only the rental records of middle 80% of customers 
left join public.payment p using (rental_id)
left join public.inventory i using (inventory_id)
where extract(year from date(rental_date)) = '2005'
group by 1,2
order by 1,2
;
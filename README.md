# dvd_rentals
PostgreSQL analysis on dvd rentals data 

This execise makes use of the dvd rentals Postgres database.

![ER Model](dvd-rental-db-diagram.png)

The database can be hosted locally using Docker, allowing you to execute queries against it.

## Set Up

### Prerequisites

- Docker installed on your local machine.
- Cloned the contents of this repository including the docker compose file and dvdrental folder to your machine.

### Hosting the database

Execute:

- `docker compose up`
- In a new terminal window; `docker exec -it pg_container bash`
  - This will give access to the container `pg_container`
- Set the database password inside the container; `set "PGPASSWORD=root"`
- Load the database; `pg_restore -U postgres -d dvdrental dvdrental`

### Connecting to the database

There are many options to connect to the database including:

- psql via CLI
- SQL editor such as DBeaver
- dbt
- pgAdmin

The connection details can be found in the `docker_compose.yml` file. The host name will likely be `localhost`.

5 questions answered: 
1. Top 10 most popular movies from rentals in H1 2005, by category.
2. Average customer value per store by month for rentals in 2005. Exclude the top & bottom 10% of customers by value from the analysis.
3. Create a table, `film_recommendations`, which provides 10 film recommendations per customer. Future recommendations could be based upon a customer's previous film choices, other customer's choices etc.
4. Create a table, `customer_lifecycle`, with a primary key of `customer_id`. This table is designed to provide a holistic view of a customer's activity and should include:
    - The revenue generated in the first 30 days of the customer's life-cycle, with day 0 being their first rental date.
    - A value tier based on the first 30 day revenue.
    - The name of the first film they rented.
    - The name of the last film they rented.
    - Last rental date.
    - Avg. time between rentals.
    - Total revenue.
    - The top 3 favorite actors per customer.
    - 1 interesting dimension or fact of your choice
5. Imagine that new rental data is being loaded into the database every hour. Assuming that the data is loaded sequentially, ordered by `rental_date`, re-purpose your logic for the `customer_lifecycle` table to process the new data in an incremental manner to a new table `customer_lifecycle_incremental`.


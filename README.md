# Testing Sehatq

## Install Docker

To run apache airflow please run commands below in your command line

- `cd [into your test_sehatq folder]`
- Run`docker compose up` to running all container
- Run `docker exec -i test_sehatq_mariadb_1 bash -l -c "mysql -u root -pmariadb < /docker-entrypoint-initdb.d/sakila-schema.sql"` to import data source schema into mysql and Datawarehouse Schema
- Run `docker exec -i test_sehatq_mariadb_1 bash -l -c "mysql -u root -pmariadb < /docker-entrypoint-initdb.d/sakila-data.sql"` to import all data sakila database into data source schema
- Open the browser and type `localhost:3333` to access phpmyadmin and check the database that imported before
- Acount for logged in to database mysql is
  Username: root
  Password: mariadb
  ![Access database](/images/phpmyadmin.png)
- Access Apache Airflow website from your browser with url `http://localhost:8080/`
  ![Apache Airflow](/images/login_apache_airflow.png)
- The account that you can use to loggin into Apache Airflow is:
  Username : airflow
  Password : airflow
- After logged in, please go to `admin menu` > `Variables` then import `variables.json` from root this project
  ![Apache Airflow](/images/variables.png)
- The Variables will used to connect into database mysql
- Then go to connection and create connection for database source sakila
  connection Id: sehatq_source_db
  connection Type: MYSQL
  HOST: mariadb
  schema: sakila
  Login: root
  Password: mariadb
  ![Apache Airflow](/images/connection_source.png)
- create new connection again to connect into DW schema
  connection Id: sehatq_schema_db
  connection Type: MYSQL
  HOST: mariadb
  schema: dw_sakila
  Login: root
  Password: mariadb
  ![Apache Airflow](/images/connection_source_db.png)
- Finally please run the dags to execute pipeline star schema with only running one dag called `controller` dag. When you run `controller` dag it will trigger all dimension and fact pipline to run ETL process until saved into dw_sakila
  ![Apache Airflow](/images/controller.png)
- Final step is to check dw_sakila. it should be contain 6 tables

###### Sample queries to compare origin schema vs star schema

1. Origin Schema
   Query: `SELECT g.country, f.city, SUM(a.amount) as revenue FROM payment a INNER JOIN rental b on a.rental_id=b.rental_id INNER JOIN inventory c on c.inventory_id=b.inventory_id INNER JOIN store d on d.store_id=c.store_id INNER JOIN address e on e.address_id=d.address_id INNER JOIN city f on f.city_id=e.city_id INNER JOIN country g ON g.country_id=f.country_id GROUP BY g.country, f.city`
   Execution Time: 0.1261 Seconds
   Result:
   ![Apache Airflow](/images/origin_schema.png)
2. Star Schema
   Query: `SELECT b.store, sum(a.amount) as revenue FROM sales_fact a INNER JOIN store_dim b ON a.store_id=b.store_id GROUP by b.store`
   Execution Time: 0.0621 Seconds
   Result:
   ![Apache Airflow](/images/star_schema.png)

The differen execution time between origin schema and star schema is **0.064**. The conclution is star query from start schema faster then origin

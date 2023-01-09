
DECLARE start_date DATE DEFAULT '2022-01-01'; 
DECLARE end_date DATE DEFAULT '2022-10-05'; 



-- 1) create a customer log temp table 
create or replace table `customer-analytics-306513.adhoc.customer_log_backup`

as 

SELECT * FROM `customer-analytics-306513.customer_universe.customer_log` 
WHERE event_date >= start_date AND event_date <= end_date
and (event_type = "search" OR event_type = "null_search")
; 

-- 2) create a table that contains the epsilon_id, event_date, event_type, and event_time of records that need to be deleted
create or replace table `customer-analytics-306513.adhoc.customer_log_duplicates`

as 

		select a.epsilon_id
		, a.event_date
		, a.event_type
		, a.event_time

		from 

			(
			select * FROM `customer-analytics-306513.adhoc.customer_log_backup` a
			WHERE event_date >= start_date AND event_date <= end_date
			and (event_type = "search" OR event_type = "null_search")
			) a 

		inner join 

			(
			select epsilon_id
			, event_date
			, event_type
			, event_time
			, count(*) as count
			from 
				(
				WITH data AS 
				
					(
					SELECT epsilon_id
					, event_date
					, event_type
					, event_time
					, ARRAY((SELECT x.key FROM UNNEST(a.event_detail) x where x.key = "banner")) as r
					FROM `customer-analytics-306513.adhoc.customer_log_backup` a
					WHERE event_date >= start_date AND event_date <= end_date
					and (event_type = "search" OR event_type = "null_search")
					)

				SELECT
				*
				FROM data,
				UNNEST(r) r
				)
			group by 1, 2, 3, 4
			) b

		on a.epsilon_id = b.epsilon_id
		and a.event_date = b.event_date
		and a.event_type = b.event_type
		and a.event_time = b.event_time

		where count > 1
; 

-- 3) double check the customer_log_duplicates table
    select * from `customer-analytics-306513.adhoc.customer_log_duplicates`
; 

-- 4) delete the duplicate records in the temporary customer_log_backup table
	delete from `customer-analytics-306513.adhoc.customer_log_backup`  a 
	where a.epsilon_id in (select distinct epsilon_id from `customer-analytics-306513.adhoc.customer_log_duplicates`) 
	and a.event_date in (select distinct event_date from `customer-analytics-306513.adhoc.customer_log_duplicates`) 
	and a.event_type in (select distinct event_type from `customer-analytics-306513.adhoc.customer_log_duplicates`) 
	and a.event_time in (select distinct event_time from `customer-analytics-306513.adhoc.customer_log_duplicates`)
	and event_date >= start_date AND event_date <= end_date
; 
	
	
-- 4) delete the duplicate records in the actual customer_log table
	delete from `customer-analytics-306513.customer_universe.customer_log`  a 
	where a.epsilon_id in (select distinct epsilon_id from `customer-analytics-306513.adhoc.customer_log_duplicates`) 
	and a.event_date in (select distinct event_date from `customer-analytics-306513.adhoc.customer_log_duplicates`) 
	and a.event_type in (select distinct event_type from `customer-analytics-306513.adhoc.customer_log_duplicates`) 
	and a.event_time in (select distinct event_time from `customer-analytics-306513.adhoc.customer_log_duplicates`)
	and event_date >= start_date AND event_date <= end_date
; 




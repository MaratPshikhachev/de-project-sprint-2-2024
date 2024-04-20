-- DDL витрины данных по заказчикам
DROP TABLE IF EXISTS dwh.customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
		id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
		customer_id INT8 NOT NULL,
		customer_name VARCHAR NOT NULL,
		customer_address varchar NOT NULL,
		customer_birthday date NOT NULL,
		customer_email varchar NOT NULL,
		customer_total_payment numeric(15, 2) NOT NULL,
		platform_money int8 NOT NULL,
		count_order int8 NOT NULL,
		avg_price_order numeric(10, 2) NOT NULL,
		median_time_order_completed numeric(10, 1) NULL,
		top_product_category varchar NOT NULL,
		top_craftsman_id INT8 NOT null,
		count_order_created int8 NOT NULL,
		count_order_in_progress int8 NOT NULL,
		count_order_delivery int8 NOT NULL,
		count_order_done int8 NOT NULL,
		count_order_not_done int8 NOT NULL,
		report_period varchar NOT NULL,
		CONSTRAINT customer_report_datamart_pk PRIMARY KEY (id)
);

-- DDL таблицы инкрементальных загрузок
DROP TABLE IF EXISTS dwh.load_dates_customer_report_datamart;

CREATE TABLE IF NOT EXISTS dwh.load_dates_customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    load_dttm DATE NOT NULL,
    CONSTRAINT load_dates_customer_report_datamart_pk PRIMARY KEY (id)
);

-- инкрементальное обновление витрины по заказчикам
WITH 
	dwh_delta AS ( -- собираем дельту изменений (изменённые в витрине или добавленные данные в DWH). 
				SELECT 
					dc.customer_id AS customer_id,
					dc.customer_name AS customer_name,
					dc.customer_address AS customer_address,
					dc.customer_birthday AS customer_birthday,
					dc.customer_email AS customer_email,
					fo.order_id AS order_id,
					dp.product_id AS product_id,
					dp.product_price AS product_price,
					dp.product_type AS product_type,
					fo.order_completion_date - fo.order_created_date AS diff_order_date,
					fo.order_status AS order_status,
					dcs.craftsman_id AS craftsman_id,
					to_char(fo.order_created_date, 'yyyy-mm') AS report_period,
					crd.customer_id AS exist_customer_id,
					dc.load_dttm AS customer_load_dttm,
					dcs.load_dttm AS crastman_load_dttm,
					dp.load_dttm AS products_load_dttm
				FROM dwh.f_order fo 
				INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id 
				INNER JOIN dwh.d_craftsman dcs ON fo.craftsman_id = dcs.craftsman_id 
				INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
				LEFT JOIN dwh.customer_report_datamart crd ON crd.customer_id = dc.customer_id 
				WHERE  (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) 
					OR (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) 
					OR (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))	
					OR (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))
					),
	dwh_update_delta AS ( -- клиенты, по которым были изменения и по которым нужно обновить данные в витрине
				SELECT 
					dd.exist_customer_id AS customer_id
				FROM dwh_delta dd
				WHERE dd.exist_customer_id IS NOT NULL
				),
	dwh_delta_insert_result AS ( -- собираем новые данные для вставки в витрину
				SELECT 
					T2.customer_id AS customer_id, 
					T2.customer_name AS customer_name, 
					T2.customer_address AS customer_address, 
					T2.customer_birthday AS customer_birthday, 
					T2.customer_email AS customer_email, 
					T2.customer_total_payment AS customer_total_payment,
					T2.platform_money AS platform_money,
					T2.count_order AS count_order,
					T2.avg_price_order AS avg_price_order,
					T2.median_time_order_completed AS median_time_order_completed,
					T4.product_type AS top_product_category,
					T6.craftsman_id AS top_craftsman_id,
					T2.count_order_created AS count_order_created,  
					T2.count_order_in_progress AS count_order_in_progress, 
					T2.count_order_delivery AS count_order_delivery, 
					T2.count_order_done AS count_order_done, 
					T2.count_order_not_done AS count_order_not_done,
					T2.report_period AS report_period
				FROM (
					SELECT  -- основной набор информации
							 T1.customer_id AS customer_id, 
							 T1.customer_name AS customer_name, 
							 T1.customer_address AS customer_address, 
							 T1.customer_birthday AS customer_birthday, 
							 T1.customer_email AS customer_email, 
							 T1.report_period AS report_period,
							 SUM(T1.product_price) AS customer_total_payment,
							 SUM(T1.product_price)*0.1 AS platform_money,
							 COUNT(T1.order_id) AS count_order,
							 AVG(T1.product_price) AS avg_price_order,
							 percentile_cont(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
							 SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,  
							 SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
							 SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
							 SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
							 SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done
					FROM dwh_delta T1
					WHERE T1.exist_customer_id IS NULL
					GROUP BY T1.customer_id, 
							 T1.customer_name, 
							 T1.customer_address, 
							 T1.customer_birthday, 
							 T1.customer_email, 
							 T1.report_period
					) AS T2 
				INNER JOIN 
					(SELECT -- определение самого популярного товара для каждого покупателя
							*, 
						   ROW_NUMBER() OVER(PARTITION BY T3.customer_id ORDER BY count_product DESC) AS rank_count_product
					FROM
						(SELECT 
							dd.customer_id,
							dd.product_type,
							count(dd.product_id) AS count_product
						FROM dwh_delta dd
						GROUP BY dd.customer_id, dd.product_type
						ORDER BY count_product DESC) AS T3
						) AS T4
					ON T2.customer_id = T4.customer_id
				INNER JOIN 
					(SELECT -- определение самого популярного мастера для каждого покупателя
							*, 
							ROW_NUMBER() OVER(PARTITION BY T5.customer_id ORDER BY count_order DESC) AS rank_count_order
					FROM
						(SELECT
							dd.customer_id,
							dd.craftsman_id,
							count(dd.order_id) AS count_order
						FROM dwh_delta dd
						GROUP BY dd.customer_id, dd.craftsman_id
						ORDER BY count_order DESC) AS T5
					) AS T6
					ON T2.customer_id = T6.customer_id
				WHERE T4.rank_count_product = 1
					AND T6.rank_count_order = 1
				ORDER BY report_period
				),
	dwh_delta_update_result AS ( -- собираем данные для обновления в витрине
				SELECT
					T2.customer_id AS customer_id, 
					T2.customer_name AS customer_name, 
					T2.customer_address AS customer_address, 
					T2.customer_birthday AS customer_birthday, 
					T2.customer_email AS customer_email, 
					T2.customer_total_payment AS customer_total_payment,
					T2.platform_money AS platform_money,
					T2.count_order AS count_order,
					T2.avg_price_order AS avg_price_order,
					T2.median_time_order_completed AS median_time_order_completed,
					T4.product_type AS top_product_category,
					T6.craftsman_id AS top_craftsman_id,
					T2.count_order_created AS count_order_created,  
					T2.count_order_in_progress AS count_order_in_progress, 
					T2.count_order_delivery AS count_order_delivery, 
					T2.count_order_done AS count_order_done, 
					T2.count_order_not_done AS count_order_not_done,
					T2.report_period AS report_period
				FROM (
					SELECT  -- основной набор информации
						T1.customer_id AS customer_id, 
						T1.customer_name AS customer_name, 
						T1.customer_address AS customer_address, 
						T1.customer_birthday AS customer_birthday, 
						T1.customer_email AS customer_email, 
						T1.report_period AS report_period,
						SUM(T1.product_price) AS customer_total_payment,
						SUM(T1.product_price)*0.1 AS platform_money,
						COUNT(T1.order_id) AS count_order,
						AVG(T1.product_price) AS avg_price_order,
						percentile_cont(0.5) WITHIN GROUP(ORDER BY T1.diff_order_date) AS median_time_order_completed,
						SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,  
						SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress, 
						SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery, 
						SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done, 
						SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done
					FROM (
						SELECT 
							dc.customer_id AS customer_id,
							dc.customer_name AS customer_name,
							dc.customer_address AS customer_address,
							dc.customer_birthday AS customer_birthday,
							dc.customer_email AS customer_email,
							fo.order_id AS order_id,
							dp.product_id AS product_id,
							dp.product_price AS product_price,
							dp.product_type AS product_type,
							fo.order_completion_date - fo.order_created_date AS diff_order_date,
							fo.order_status AS order_status,
							dcs.craftsman_id AS craftsman_id,
							to_char(fo.order_created_date, 'yyyy-mm') AS report_period
						FROM dwh.f_order fo 
						INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id 
						INNER JOIN dwh.d_craftsman dcs ON fo.craftsman_id = dcs.craftsman_id 
						INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
						INNER JOIN dwh_update_delta ud ON ud.customer_id = dc.customer_id) AS T1
					GROUP BY T1.customer_id, 
							 T1.customer_name, 
							 T1.customer_address, 
							 T1.customer_birthday, 
							 T1.customer_email, 
							 T1.report_period
							) AS T2
				INNER JOIN
						(SELECT -- определение самого популярного товара для каждого покупателя
								*,
								ROW_NUMBER() OVER(PARTITION BY T3.customer_id ORDER BY count_product DESC) AS rank_count_product
						 FROM
							(SELECT 
								dd.customer_id,
								dd.product_type,
								COUNT(dd.product_id) AS count_product
							FROM dwh_delta dd
							GROUP BY dd.customer_id, dd.product_type
							ORDER BY count_product DESC) AS T3
							) AS T4
					ON T2.customer_id = T4.customer_id
				INNER JOIN 
						(SELECT  -- определение самого популярного мастера для каждого покупателя
							*,
							ROW_NUMBER() OVER(PARTITION BY T5.customer_id ORDER BY count_order DESC) AS rank_count_order
				 		 FROM
							(SELECT
								dd.customer_id,
								dd.craftsman_id,
								COUNT(dd.order_id) AS count_order
					  		 FROM dwh_delta dd
							 GROUP BY dd.customer_id, dd.craftsman_id
							 ORDER BY count_order DESC) AS T5
						) AS T6
					ON T2.customer_id = T6.customer_id
				WHERE T4.rank_count_product = 1
					AND T6.rank_count_order = 1
				ORDER BY report_period
					),	
	insert_delta AS ( -- insert новых расчитанных данных для витрины
				INSERT INTO dwh.customer_report_datamart (
					customer_id,
					customer_name,
					customer_address,
					customer_birthday,
					customer_email,
					customer_total_payment,
					platform_money,
					count_order,
					avg_price_order,
					median_time_order_completed,
					top_product_category,
					top_craftsman_id,
					count_order_created,
					count_order_in_progress,
					count_order_delivery,
					count_order_done,
					count_order_not_done,
					report_period
				) SELECT 
					customer_id,
					customer_name,
					customer_address,
					customer_birthday,
					customer_email,
					customer_total_payment,
					platform_money,
					count_order,
					avg_price_order,
					median_time_order_completed,
					top_product_category,
					top_craftsman_id,
					count_order_created,
					count_order_in_progress,
					count_order_delivery,
					count_order_done,
					count_order_not_done,
					report_period
				  FROM dwh_delta_insert_result
				),
	update_delta AS ( -- обновление данных в витрине по уже существующим клиентам
				UPDATE dwh.customer_report_datamart SET
					customer_name = upd.customer_name,
					customer_address = upd.customer_address,
					customer_birthday = upd.customer_birthday,
					customer_email = upd.customer_email,
					customer_total_payment = upd.customer_total_payment,
					platform_money = upd.platform_money,
					count_order = upd.count_order,
					avg_price_order = upd.avg_price_order,
					median_time_order_completed = upd.median_time_order_completed,
					top_product_category = upd.top_product_category,
					top_craftsman_id = upd.top_craftsman_id,
					count_order_created = upd.count_order_created,
					count_order_in_progress = upd.count_order_in_progress,
					count_order_delivery = upd.count_order_delivery,
					count_order_done = upd.count_order_done,
					count_order_not_done = upd.count_order_not_done,
					report_period = upd.report_period
				FROM (
					SELECT 
						customer_id,
						customer_name,
						customer_address,
						customer_birthday,
						customer_email,
						customer_total_payment,
						platform_money,
						count_order,
						avg_price_order,
						median_time_order_completed,
						top_product_category,
						top_craftsman_id,
						count_order_created,
						count_order_in_progress,
						count_order_delivery,
						count_order_done,
						count_order_not_done,
						report_period
					FROM dwh_delta_update_result 
				) AS upd
				WHERE dwh.customer_report_datamart.customer_id = upd.customer_id
			),
	insert_load_date AS ( -- запись в таблицу загрузок о том, когда была совершена последняя загрузка
				INSERT INTO dwh.load_dates_customer_report_datamart (
						load_dttm
				)
			    SELECT GREATEST(COALESCE(MAX(customer_load_dttm), NOW()), 
			                    COALESCE(MAX(crastman_load_dttm), NOW()), 
			                    COALESCE(MAX(products_load_dttm), NOW())) 
			    FROM dwh_delta
			)
SELECT 'increment datamart'; 
# de-project-sprint-2-2023

**задача:** имеется маркетплейс товаров ручной работы. В процессе расширения в состав маркетплейса был приобретён дополнительный сайт. 
Необходимо: 
- настроить перенос данных их новго источника в хранилище 
- построить витрину данных с информацией о заказчиках и их активности на маркетплейсе.

**код SQL**
***скрипт переноса данных из источника в хранилище***
```
-- создание временной таблицы с данными из всех источников, включая новые
DROP TABLE IF EXISTS tmp_sources;
CREATE TEMP TABLE tmp_sources AS 
SELECT  order_id,
        order_created_date,
        order_completion_date,
        order_status,
        craftsman_id,
        craftsman_name,
        craftsman_address,
        craftsman_birthday,
        craftsman_email,
        product_id,
        product_name,
        product_description,
        product_type,
        product_price,
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email 
  FROM source1.craft_market_wide
UNION
SELECT  t2.order_id,
        t2.order_created_date,
        t2.order_completion_date,
        t2.order_status,
        t1.craftsman_id,
        t1.craftsman_name,
        t1.craftsman_address,
        t1.craftsman_birthday,
        t1.craftsman_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t2.customer_id,
        t2.customer_name,
        t2.customer_address,
        t2.customer_birthday,
        t2.customer_email 
  FROM source2.craft_market_masters_products t1 
    JOIN source2.craft_market_orders_customers t2 ON t2.product_id = t1.product_id AND t1.craftsman_id = t2.craftsman_id 
UNION
SELECT  t1.order_id,
        t1.order_created_date,
        t1.order_completion_date,
        t1.order_status,
        t2.craftsman_id,
        t2.craftsman_name,
        t2.craftsman_address,
        t2.craftsman_birthday,
        t2.craftsman_email,
        t1.product_id,
        t1.product_name,
        t1.product_description,
        t1.product_type,
        t1.product_price,
        t3.customer_id,
        t3.customer_name,
        t3.customer_address,
        t3.customer_birthday,
        t3.customer_email
  FROM source3.craft_market_orders t1
    JOIN source3.craft_market_craftsmans t2 ON t1.craftsman_id = t2.craftsman_id 
    JOIN source3.craft_market_customers t3 ON t1.customer_id = t3.customer_id
UNION
SELECT cpo.order_id,
        cpo.order_completion_date,
        cpo.order_completion_date,
        cpo.order_status,
        cpo.craftsman_id,
        cpo.craftsman_name,	
        cpo.craftsman_address,
        cpo.craftsman_birthday,
        cpo.craftsman_email,
        cpo.product_id,
        cpo.product_name,
        cpo.product_description,
        cpo.product_type,
        cpo.product_price,
        c.customer_id,
        c.customer_name,
        c.customer_address,
        c.customer_birthday,
        c.customer_email 
FROM external_source.craft_products_orders cpo 
JOIN external_source.customers c ON cpo.customer_id = c.customer_id;

-- обновление данных в таблицах с измерениями
-- dwh.d_craftsman d*
MERGE INTO dwh.d_craftsman d
USING (SELECT DISTINCT 
                craftsman_name,
                craftsman_address, 
                craftsman_birthday, 
                craftsman_email
        FROM tmp_sources) t
ON d.craftsman_name = t.craftsman_name 
AND d.craftsman_email = t.craftsman_email
WHEN MATCHED THEN
  UPDATE SET craftsman_address = t.craftsman_address, 
            craftsman_birthday = t.craftsman_birthday, 
            load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
  VALUES (t.craftsman_name, t.craftsman_address, t.craftsman_birthday, t.craftsman_email, current_timestamp);
-- dwh.d_product d
MERGE INTO dwh.d_product d
USING (SELECT DISTINCT 
                product_name, 
                product_description, 
                product_type, 
                product_price 
        FROM tmp_sources) t
ON d.product_name = t.product_name 
    AND d.product_description = t.product_description 
    AND d.product_price = t.product_price
WHEN MATCHED THEN
  UPDATE SET product_type = t.product_type,
          load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_name, product_description, product_type, product_price, load_dttm)
  VALUES (t.product_name, t.product_description, t.product_type, t.product_price, current_timestamp);

-- dwh.d_customer d
 MERGE INTO dwh.d_customer d
USING (SELECT DISTINCT 
                customer_name, 
                customer_address, 
                customer_birthday, 
                customer_email 
        FROM tmp_sources) t
ON d.customer_name = t.customer_name 
  AND d.customer_email = t.customer_email
WHEN MATCHED THEN
  UPDATE SET customer_address = t.customer_address, 
             customer_birthday= t.customer_birthday, 
             load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
  VALUES (t.customer_name, t.customer_address, t.customer_birthday, t.customer_email, current_timestamp);
  
 -- создание временной таблицы tmp_sources_fact
DROP TABLE IF EXISTS tmp_sources_fact;
CREATE TEMP TABLE tmp_sources_fact AS 
SELECT  dp.product_id,
        dc.craftsman_id,
        dcust.customer_id,
        src.order_created_date,
        src.order_completion_date,
        src.order_status,
        dp.load_dttm
FROM tmp_sources src
JOIN dwh.d_craftsman dc ON dc.craftsman_name = src.craftsman_name 
                        AND dc.craftsman_email = src.craftsman_email 
JOIN dwh.d_customer dcust ON dcust.customer_name = src.customer_name 
                        AND dcust.customer_email = src.customer_email 
JOIN dwh.d_product dp ON dp.product_name = src.product_name 
                    AND dp.product_description = src.product_description 
                    AND dp.product_price = src.product_price;
                    
-- обновление в f_order
MERGE INTO dwh.f_order f
USING tmp_sources_fact t
ON f.product_id = t.product_id 
    AND f.craftsman_id = t.craftsman_id 
    AND f.customer_id = t.customer_id 
    AND f.order_created_date = t.order_created_date 
WHEN MATCHED THEN
  UPDATE SET order_completion_date = t.order_completion_date, 
          order_status = t.order_status, 
          load_dttm = current_timestamp
WHEN NOT MATCHED THEN
  INSERT (product_id, 
          craftsman_id, 
          customer_id, 
          order_created_date, 
          order_completion_date, 
          order_status, 
          load_dttm)
  VALUES (t.product_id, 
          t.craftsman_id, 
          t.customer_id, 
          t.order_created_date, 
          t.order_completion_date, 
          t.order_status, 
          current_timestamp);
```
***составление новой витрины и скрипт для инкрементального обновления витрины***
```
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
```

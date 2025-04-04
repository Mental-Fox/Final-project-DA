CREATE DATABASE final_project;
USE final_project;

-- Создаем таблицы transactions_final и customer_final
CREATE TABLE transactions_final (
    date_new DATE,
    Id_check INT,
    ID_client INT,
    Count_products DECIMAL(10,3),
    Sum_payment DECIMAL(10,2)
);

CREATE TABLE customer_final (
    Id_client INT PRIMARY KEY,
    Total_amount DECIMAL(10,2),
    Gender VARCHAR(10),
    Age INT,
    Count_city INT,
    Response_communcation INT,
    Communication_3month INT,
    Tenure INT
);

-- Отключаем безопасный режим обновлений
SET SQL_SAFE_UPDATES = 0;

-- Обновляем пустые значения Gender на NULL
-- Обновляем значения Age: заменяем пустые строки или нечисловые значения на NULL
UPDATE customer_final SET Gender = NULL WHERE Gender = '';
UPDATE customer_final SET Age = NULL WHERE Age = '' OR Age REGEXP '[^0-9]';


-- Изменяем тип данных Age на VARCHAR(10), чтобы поддерживать текстовые значения
ALTER TABLE customer_final MODIFY Age VARCHAR(10);

-- Загружаем данные в таблицу transactions_final
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions_final.csv'
INTO TABLE transactions_final
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Загружаем данные в таблицу customer_final
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\customer_final.csv'
INTO TABLE customer_final
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Проверяем настройки secure_file_priv
SHOW VARIABLES LIKE 'secure_file_priv';

-- Проверяем, загрузились ли данные
SELECT * FROM transactions_final LIMIT 10;

-- Задание 1: Анализ клиентов с покупками каждый месяц за год (июнь 2015 - июнь 2016)
WITH MonthlyActivity AS (
    -- Считаем активность клиентов по месяцам
    SELECT 
        ID_client,
        EXTRACT(YEAR FROM date_new) AS transaction_year,
        EXTRACT(MONTH FROM date_new) AS transaction_month
    FROM transactions_final
    WHERE date_new >= '2015-06-01' AND date_new <= '2016-06-01'
    GROUP BY ID_client, transaction_year, transaction_month
),
ConsistentClients AS (
    -- Выбираем клиентов, у которых есть покупки в каждом из 12 месяцев
    SELECT ID_client
    FROM MonthlyActivity
    GROUP BY ID_client
    HAVING COUNT(DISTINCT CONCAT(transaction_year, '-', transaction_month)) = 12
),
ClientMetrics AS (
    -- Рассчитываем ключевые показатели для постоянных клиентов
    SELECT 
        t.ID_client,
        COUNT(t.Id_check) AS transaction_count,
        SUM(t.Sum_payment) AS total_purchases,
        AVG(t.Sum_payment) AS average_check,
        SUM(t.Sum_payment) / 12 AS monthly_average_spend
    FROM transactions_final t
    INNER JOIN ConsistentClients cc ON t.ID_client = cc.ID_client
    WHERE t.date_new >= '2015-06-01' AND t.date_new <= '2016-06-01'
    GROUP BY t.ID_client
)
-- Выводим результат с данными клиентов
SELECT 
    c.ID_client AS client_id,
    cm.total_purchases AS total_spent,
    c.Gender AS gender,
    c.Age AS age,
    c.Count_city AS city_count,
    c.Response_communcation AS communication_response,
    c.Communication_3month AS recent_communication,
    c.Tenure AS tenure,
    cm.transaction_count AS total_transactions,
    cm.total_purchases AS purchase_sum,
    cm.average_check AS avg_receipt,
    cm.monthly_average_spend AS avg_per_month
FROM ClientMetrics cm
INNER JOIN customer_final c ON cm.ID_client = c.ID_client
ORDER BY cm.total_purchases DESC;


-- Задание 2: Анализ транзакций по месяцам с учетом пола
WITH PerMonthStats AS (
    -- Считаем общую статистику по месяцам
    SELECT 
        EXTRACT(YEAR FROM date_new) AS yr,
        EXTRACT(MONTH FROM date_new) AS mth,
        COUNT(Id_check) AS operation_count,
        COUNT(DISTINCT ID_client) AS distinct_clients,
        SUM(Sum_payment) AS revenue_total,
        AVG(Sum_payment) AS check_average,
        100 * COUNT(Id_check) / SUM(COUNT(Id_check)) OVER () AS pct_operations,
        100 * SUM(Sum_payment) / SUM(SUM(Sum_payment)) OVER () AS pct_revenue
    FROM transactions_final
    WHERE date_new >= '2015-06-01' AND date_new <= '2016-06-01'
    GROUP BY yr, mth
),
GenderBreakdown AS (
    -- Анализируем данные по полу
    SELECT 
        EXTRACT(YEAR FROM t.date_new) AS yr,
        EXTRACT(MONTH FROM t.date_new) AS mth,
        c.Gender AS gender,
        COUNT(DISTINCT t.ID_client) AS gender_client_count,
        SUM(t.Sum_payment) AS gender_total_spent,
        100 * COUNT(DISTINCT t.ID_client) / SUM(COUNT(DISTINCT t.ID_client)) OVER (PARTITION BY EXTRACT(YEAR FROM t.date_new), EXTRACT(MONTH FROM t.date_new)) AS gender_pct_clients,
        100 * SUM(t.Sum_payment) / SUM(SUM(t.Sum_payment)) OVER (PARTITION BY EXTRACT(YEAR FROM t.date_new), EXTRACT(MONTH FROM t.date_new)) AS gender_pct_spending
    FROM transactions_final t
    INNER JOIN customer_final c ON t.ID_client = c.ID_client
    WHERE t.date_new >= '2015-06-01' AND t.date_new <= '2016-06-01'
    GROUP BY yr, mth, c.Gender
)
-- Объединяем данные и выводим результат
SELECT 
    pms.yr AS year,
    pms.mth AS month,
    pms.check_average AS monthly_avg_check,
    pms.operation_count / 12 AS avg_monthly_operations,
    pms.distinct_clients / 12 AS avg_monthly_clients,
    pms.pct_operations AS yearly_operation_share,
    pms.pct_revenue AS yearly_revenue_share,
    gb.gender AS client_gender,
    gb.gender_client_count AS clients_by_gender,
    gb.gender_pct_clients AS gender_client_percentage,
    gb.gender_pct_spending AS gender_spending_percentage
FROM PerMonthStats pms
LEFT JOIN GenderBreakdown gb ON pms.yr = gb.yr AND pms.mth = gb.mth
ORDER BY pms.yr, pms.mth, gb.gender;


-- Задание 3: Анализ транзакций по возрастным группам и кварталам
WITH ClientAgeCategories AS (
    -- Определяем возрастные группы клиентов
    SELECT 
        ID_client,
        CASE 
            WHEN Age IS NULL THEN 'Unknown'
            WHEN CAST(Age AS UNSIGNED) < 10 THEN '0-9'
            WHEN CAST(Age AS UNSIGNED) < 20 THEN '10-19'
            WHEN CAST(Age AS UNSIGNED) < 30 THEN '20-29'
            WHEN CAST(Age AS UNSIGNED) < 40 THEN '30-39'
            WHEN CAST(Age AS UNSIGNED) < 50 THEN '40-49'
            WHEN CAST(Age AS UNSIGNED) < 60 THEN '50-59'
            ELSE '60+'
        END AS age_category
    FROM customer_final
),
OverallStats AS (
    -- Считаем общие показатели по возрастным группам
    SELECT 
        cac.age_category,
        SUM(t.Sum_payment) AS overall_spending,
        COUNT(t.ID_check) AS overall_transactions
    FROM transactions_final t
    INNER JOIN ClientAgeCategories cac ON t.ID_client = cac.ID_client
    GROUP BY cac.age_category
),
QuarterlyBreakdown AS (
    -- Анализируем данные по кварталам
    SELECT 
        cac.age_category,
        EXTRACT(YEAR FROM t.date_new) AS yr,
        QUARTER(t.date_new) AS qtr,
        COUNT(t.ID_check) AS quarterly_transactions,
        SUM(t.Sum_payment) AS quarterly_spending,
        AVG(t.Sum_payment) AS quarterly_avg_check,
        100 * SUM(t.Sum_payment) / SUM(SUM(t.Sum_payment)) OVER (PARTITION BY EXTRACT(YEAR FROM t.date_new), QUARTER(t.date_new)) AS quarterly_revenue_pct
    FROM transactions_final t
    INNER JOIN ClientAgeCategories cac ON t.ID_client = cac.ID_client
    GROUP BY cac.age_category, yr, qtr
)
-- Выводим итоговые данные
SELECT 
    os.age_category AS age_group,
    os.overall_spending AS total_spent,
    os.overall_transactions AS total_ops,
    qb.yr AS year,
    qb.qtr AS quarter,
    qb.quarterly_transactions AS ops_per_quarter,
    qb.quarterly_spending AS spent_per_quarter,
    qb.quarterly_avg_check AS avg_check_quarter,
    qb.quarterly_revenue_pct AS revenue_share_quarter
FROM OverallStats os
LEFT JOIN QuarterlyBreakdown qb ON os.age_category = qb.age_category
ORDER BY qb.yr, qb.qtr, os.age_category;
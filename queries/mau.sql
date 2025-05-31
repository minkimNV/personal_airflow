WITH B_USER_DATA AS (
    SELECT
        DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul")) AS date,
        EXTRACT(YEAR FROM DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul"))) AS year,
        EXTRACT(MONTH FROM DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul"))) AS month,
        DATE(CAST(EXTRACT(YEAR FROM DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul"))) AS STRING) || '-' || 
                LPAD(CAST(EXTRACT(MONTH FROM DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul"))) AS STRING), 2, '0') || '-01') AS year_month_date,

        email,
        user_id,
        event_name
    FROM `*****.*****_analytics.000_*****`
)

-- GA 로그데이터
, GA_LOG_DATA AS (
    SELECT
        DATE(CAST(EXTRACT(YEAR FROM DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul"))) AS STRING) || '-' || 
                LPAD(CAST(EXTRACT(MONTH FROM DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul"))) AS STRING), 2, '0') || '-01') AS year_month_date,
        user_id,
        event_name,
        page_path,
        engagement_time_sec
    FROM `*****.*****_analytics.001_*****`
)

-- MAU
, B_GA_MAU AS (
    SELECT
        year_month_date, COUNT(DISTINCT user_id) AS MAU
    FROM B_USER_DATA
    GROUP BY year_month_date

    UNION ALL

    SELECT
        year_month_date,
        COUNT(DISTINCT CASE WHEN ...
                                AND (
                                ...
                                ) THEN user_id END) AS MAU
    FROM GA_LOG_DATA
    GROUP BY year_month_date
)

-- VISITOR-TO-LEAD CONVERSION
, GA_FIRST_VISIT AS (
    SELECT
        GA.year_month_date,
        COUNT(DISTINCT CASE WHEN event_name = 'first_visit'
                        OR event_name = 'session_start' 
                        THEN user_id END) AS first_visit
    FROM GA_LOG_DATA GA
    GROUP BY year_month_date
)

-- 결과 병합 (FULL OUTER JOIN)
SELECT
    mau.year_month_date,
    COALESCE(mau.MAU, 0) AS MAU,
    COALESCE(FV.first_visit, 0) AS first_visit
FROM B_GA_MAU mau
FULL OUTER JOIN GA_FIRST_VISIT FV ON mau.year_month_date = FV.year_month_date
ORDER BY year_month_date

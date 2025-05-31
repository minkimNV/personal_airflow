WITH B_USER_DATA AS (
    SELECT
        DATE_TRUNC(DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul")), WEEK(MONDAY)) AS week_start_date,
        email,
        user_id,
        event_name
    FROM `*****.*****_analytics.000_*****`
)

-- GA 로그데이터
, GA_LOG_DATA AS (
    SELECT
        DATE_TRUNC(DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul")), WEEK(MONDAY)) AS week_start_date,
        user_id,
        event_name,
        page_path,
        engagement_time_sec
    FROM `*****.*****_analytics.001_*****`
)

-- WAU (Weekly Active Users) -> 주별로 변경
, B_GA_WAU AS (
    SELECT
        week_start_date, 
        COUNT(DISTINCT user_id) AS B_WAU
    FROM B_USER_DATA
    GROUP BY week_start_date

    UNION ALL

    SELECT
        week_start_date,
        COUNT(DISTINCT CASE WHEN ...
                                AND (
                                    ...
                                ) THEN user_id END) AS GA_WAU
    FROM GA_LOG_DATA
    GROUP BY week_start_date
)

-- VISITOR-TO-LEAD CONVERSION 
, GA_FIRST_VISIT AS (
    SELECT
        GA.week_start_date,
        COUNT(DISTINCT CASE WHEN event_name = 'first_visit'
                            OR event_name = 'session_start' THEN user_id END) AS first_visit
    FROM GA_LOG_DATA GA
    GROUP BY week_start_date
)

-- 결과 병합 (FULL OUTER JOIN) -> 주별로 변경
SELECT
    wau.week_start_date,
    COALESCE(wau.B_WAU, 0) AS WAU,
    COALESCE(FV.first_visit, 0) AS first_visit
FROM B_GA_WAU wau
FULL OUTER JOIN GA_FIRST_VISIT FV ON wau.week_start_date = FV.week_start_date
ORDER BY week_start_date

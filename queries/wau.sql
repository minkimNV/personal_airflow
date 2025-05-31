WITH B_USER_DATA AS (
    SELECT
        DATE_TRUNC(DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul")), WEEK(MONDAY)) AS week_start_date,
        email,
        user_id,
        event_name
    FROM `modern-tangent-398308.deepsales_analytics.000_User_Info_Before_240901`
)

-- GA 로그데이터
, GA_LOG_DATA AS (
    SELECT
        DATE_TRUNC(DATE(DATETIME(TIMESTAMP(event_time_utc), "Asia/Seoul")), WEEK(MONDAY)) AS week_start_date,
        user_id,
        event_name,
        page_path,
        engagement_time_sec
    FROM `modern-tangent-398308.analytics_290195897.cleaned_events_log`
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
        COUNT(DISTINCT CASE WHEN page_path NOT LIKE '%login?r=%' 
                                AND page_path NOT LIKE '%login?page=signup%'
                                AND (
                                    page_path LIKE '%prospecting/contacts%'
                                    OR page_path LIKE '%prospecting/companies%'
                                    OR page_path LIKE '%tutorial%'
                                    OR page_path LIKE '%discover%'
                                    OR page_path LIKE '%company/setup%'
                                    OR page_path LIKE '%account/%'
                                    OR page_path LIKE '%mylists%'
                                    OR page_path LIKE '%company/019%'
                                    OR page_path LIKE '%pricing%'
                                    OR page_path LIKE '%payment%'
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
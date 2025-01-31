drop table if exists y_nishio.validation_data_to_compute_ema;
create table y_nishio.validation_data_to_compute_ema as 
/* 計算期間リストの作成(本日の日付より過去3ヶ月前)*/
with day_list  as 
(SELECT
    CAST(DATE_TRUNC('month', target) AS date) AS ym,
    CAST(date_format(target, '%Y-%m-%d') AS date) AS dt
FROM
    (SELECT sequence(
        /*date_trunc('month', current_date) - interval '2' month, proto*/
        date_trunc('day', current_date) - interval '1' day, /*validation test*/
        current_date,
        interval '1' day
    ) AS targets
    FROM (VALUES 1)
    ) AS subquery
CROSS JOIN UNNEST(targets) AS t(target)
),

/*調査作成日起点で期間内に配信された調査IDに対して回答したか否かデータの作成*/
response_data_for_past_three_month as (
  SELECT
    pp.mm_panelist_id,
    pp.mm_project_id,
    DATE_TRUNC('month', pp.create_dt) as ym,
    CASE
      WHEN fa.finished_dt IS NULL THEN 0
      ELSE 1
    END AS ans_flg,
    fa.finished_dt,
    pp.create_dt
  FROM
    transam.project_panelist pp
    LEFT JOIN transam.finish_answer fa
      ON pp.mm_panelist_id = fa.mm_panelist_id
      AND pp.mm_project_id = fa.mm_project_id
  WHERE EXISTS (
    SELECT *
    FROM day_list
    WHERE day_list.dt = date_trunc('day', pp.create_dt)
  )
),

/*現日付から過去3ヶ月間(検証では，5日)デイリーの回答率を計算*/
daily_counts AS (
    SELECT
        rd_three.mm_panelist_id,
        DATE_TRUNC('day', rd_three.create_dt) AS create_dt,
        CAST(SUM(CASE WHEN mm_project_id IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL(10,3)) AS pjt_count, -- FLOATに一旦変換
        CAST(SUM(CASE WHEN rd_three.ans_flg = 1 THEN 1 ELSE 0 END) AS DECIMAL(10,3)) AS ans_count -- FLOATに一旦変換
    FROM 
        response_data_for_past_three_month rd_three
    GROUP BY
        DATE_TRUNC('day', rd_three.create_dt),
        rd_three.mm_panelist_id
)


/*全モニタへの現日付より過去3ヶ月回答有無情報の付与*/
SELECT
    pane.mm_panelist_id,
    dc.create_dt,
    CAST(COALESCE(dc.pjt_count, 0) AS INT) AS pjt_count,
    CAST(COALESCE(dc.ans_count, 0) AS INT) AS ans_count,
    CASE
        WHEN COALESCE(dc.pjt_count, 0) > 0 THEN COALESCE(dc.ans_count,0)/COALESCE(dc.pjt_count,0)
        ELSE 0
    END AS ans_ratio 
FROM transam.panelist pane
LEFT JOIN
    daily_counts dc
ON pane.mm_panelist_id = dc.mm_panelist_id
ORDER BY
    pane.mm_panelist_id, dc.create_dt;
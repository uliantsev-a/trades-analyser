# project/api/queries.py
# This file need for storing sql queries, so as not to litter another space


DELTA_SELECT = '''
    WITH tab_diff AS (
        SELECT
        price_history_1.date as history_begin, price_history_2.date as history_end,
        abs(price_history_1.{type_price} - price_history_2.{type_price}) as diff,
        price_history_1.open as value,
        price_history_2.open as value2,
        ticker.id as ticker_id
        FROM ticker JOIN price_history AS price_history_1 ON price_history_1.ticker_id = ticker.id
        JOIN price_history AS price_history_2
            ON price_history_2.ticker_id = ticker.id
            AND price_history_2.date  > price_history_1.date
        WHERE ticker.name = '{ticker_name}'
    ),
    end_group AS (
        SELECT
        tab_diff.history_begin,
        MIN(tab_diff.history_end) OVER (PARTITION BY tab_diff.history_begin) as end_date
        FROM tab_diff WHERE diff > {value_delta}
    ),
    begin_group AS (
        SELECT
        end_group.end_date,
        MAX(end_group.history_begin) OVER (PARTITION BY end_group.end_date) as begin_date
        FROM end_group
    ),
    period_groups AS (
        SELECT
        row_number() OVER () as g_num,
        begin_date AS begin, end_date AS end
        FROM begin_group GROUP BY end_date, begin_date
    )
    SELECT * FROM price_history as ph
    JOIN ticker AS tk ON ph.ticker_id = tk.id and tk.name = '{ticker_name}'
    JOIN period_groups AS gp ON ph.date BETWEEN gp.begin and gp.end
    JOIN tab_diff AS td ON td.history_begin = gp.begin AND td.history_end = gp.end AND  td.ticker_id = tk.id
    ORDER BY g_num, date;
'''

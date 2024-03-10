CREATE VIEW transaction_util
AS (
	WITH transactions_by_date(f_date, transactions_count, at_size) AS (
		SELECT 
			TO_DATE(date_oil, 'MM/DD/YYYY') AS f_date,
			SUM(transactions) AS transactions_count,
			SUM(unit_sales) AS at_size
		FROM public.original_data d
		GROUP BY f_date
	),
	transactions_prev(f_date, transactions_count, tc_prev, size_per_t) AS (
		SELECT
			f_date,
			transactions_count,
			LAG(transactions_count, 1) OVER (ORDER BY f_date) tc_prev,
			transactions_count/at_size AS size_per_t
		FROM transactions_by_date
		WHERE transactions_count IS NOT NULL
	)
	SELECT
		f_date,
        transactions_count,
		(transactions_count-tc_prev) as diff,
		((transactions_count-tc_prev)/tc_prev) as pct_change
	FROM transactions_prev 
	ORDER BY f_date
)
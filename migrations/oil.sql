CREATE VIEW oil_price_util
AS (
	WITH oil_price_as_date(price, f_date, transactions_count, at_size) AS (
		SELECT 
			MIN(oil_price) AS price,
			TO_DATE(date_oil, 'MM/DD/YYYY') AS f_date,
			SUM(transactions) AS transactions_count,
			SUM(unit_sales) AS at_size
		FROM public.original_data d
		GROUP BY f_date
	),
	oil_price_prev(price, f_date, prev_price, transactions_count, tc_prev, size_per_t) AS (
		SELECT
			price,
			f_date,
			LAG(price, 1) OVER (ORDER BY f_date) prev_price,
			transactions_count,
			LAG(transactions_count, 1) OVER (ORDER BY f_date) tc_prev,
			transactions_count/at_size AS size_per_t
		FROM oil_price_as_date
		WHERE price IS NOT NULL
	)
	SELECT
		price,
		f_date,
		(o.price-o.prev_price) as diff,
		((o.price-o.prev_price)/o.prev_price) as pct_change,
		(ln(o.price) - ln(o.prev_price)) as log_return
	FROM oil_price_prev o
	ORDER BY f_date
)
/*
	All the sent request during the given period.
	Including:
	- c1_01
	- c2_01
	- a3_01
	- with create_date within the period
	TODO: To be honest it should be the upload date not the creation date
*/
SELECT
	COUNT(*) AS nreq,
	dist.id AS distriid,
	dist.ref AS refdistribuidora,
	tar.name as tarname,
	provincia.code AS codiprovincia,
	provincia.name AS nomprovincia,
	dist.name AS distriname,
	STRING_AGG(sw.id::text, ',' ORDER BY sw.id) AS casos
FROM
	(
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipo_cambio,
		'c1' AS process
	FROM giscedata_switching_c1_01
	WHERE
		create_date >= %(periodStart)s AND
		create_date < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipo_cambio,
		'c2' AS process
	FROM giscedata_switching_c2_01
	WHERE
		create_date >= %(periodStart)s AND
		create_date < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C4' AS tipo_cambio,
		'a3' AS process
	FROM giscedata_switching_a3_01
	WHERE
		create_date >= %(periodStart)s AND
		create_date < %(periodEnd)s AND
		TRUE
	) AS step
LEFT JOIN
	giscedata_switching_step_header AS sth ON step.header_id = sth.id
LEFT JOIN
	giscedata_switching AS sw ON sw.id = sth.sw_id
LEFT JOIN
	giscedata_cups_ps AS cups ON cups.id = sw.cups_id
LEFT JOIN
	giscedata_polissa AS pol ON pol.id = sw.cups_polissa_id
LEFT JOIN
	res_partner AS dist ON dist.id = pol.distribuidora
LEFT JOIN
	giscedata_polissa_tarifa AS tar ON tar.id = pol.tarifa
LEFT JOIN
	res_municipi ON res_municipi.id = cups.id_municipi
LEFT JOIN
	res_country_state AS provincia ON provincia.id = res_municipi.state
GROUP BY
	dist.id,
	dist.ref,
	dist.name,
	provincia.code,
	provincia.name,
	tar.name,
	TRUE
ORDER BY
	dist.name,
	provincia.code,
	tar.name,
	TRUE
;


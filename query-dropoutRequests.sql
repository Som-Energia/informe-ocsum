/*
	Drop outs
	Including:
	- C1_06: Third party comercializer change activated
	- C2_06: Third party comercializer change activated, with contract changes
	- B3_??: TODO: Dropout to the reference comercializera
	Implemented as:
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
		1 AS process
	FROM giscedata_switching_c1_06
	WHERE
		data_activacio >= %(periodStart)s AND
		data_activacio < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		2 AS process
	FROM giscedata_switching_c2_06
	WHERE
		data_activacio >= %(periodStart)s AND
		data_activacio < %(periodEnd)s AND
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


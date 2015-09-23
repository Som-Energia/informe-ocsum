/*
	All the requests with activation date during the period
	Implemented as:
	- c1_05
	- c2_05
	- a3_05
	- with data_activacio during the period

TODO: Counting issues

*/
SELECT
	dist.id AS distriid,
	dist.ref AS refdistribuidora,
	provincia.code AS codiprovincia,
	tar.name as tarname,
	step.tipocambio,
	tipopunto,
	COUNT(*),
	SUM(CASE WHEN (
		data_activacio <= sw.create_date + interval '66 days'
		) THEN 1 ELSE 0 END) AS ontime,
	SUM(CASE WHEN (
		data_activacio >  sw.create_date + interval '66 days' AND 
		data_activacio <= sw.create_date + interval '81 days'
		) THEN 1 ELSE 0 END) AS late,
	SUM(CASE WHEN (
		data_activacio >  sw.create_date + interval '81 days'
		) THEN 1 ELSE 0 END) AS verylate,
	SUM(CASE WHEN (
		data_activacio <= sw.create_date + interval '66 days'
		) THEN DATE_PART('day', data_activacio - sw.create_date ) ELSE 0 END
	) AS ontimeaddedtime,
	SUM(CASE WHEN (
		data_activacio >  sw.create_date + interval '66 days' AND 
		data_activacio <= sw.create_date + interval '81 days'
		) THEN DATE_PART('day', data_activacio - sw.create_date ) ELSE 0 END
	) AS lateaddedtime,
	SUM(CASE WHEN (
		data_activacio >  sw.create_date + interval '81 days'
		) THEN DATE_PART('day', data_activacio - sw.create_date ) ELSE 0 END
	) AS verylateaddedtime,
	0 AS ontimeissues,
	0 AS lateissues,
	0 AS verylateissues,
	provincia.name AS nomprovincia,
	dist.name AS distriname,
	STRING_AGG(sw.id::text, ',' ORDER BY sw.id) AS casos
FROM
	(
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		data_activacio,
		'c1' AS process
	FROM giscedata_switching_c1_05
	WHERE
		data_activacio >= %(periodStart)s AND
		data_activacio < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		data_activacio,
		'c2' AS process
	FROM giscedata_switching_c2_05
	WHERE
		data_activacio >= %(periodStart)s AND
		data_activacio < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C4' AS tipocambio,
		data_activacio,
		'a3' AS process
	FROM giscedata_switching_a3_05
	WHERE
		data_activacio >= %(periodStart)s AND
		data_activacio < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		data_activacio,
		'c2' AS process
	FROM giscedata_switching_c2_07
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
	res_municipi ON res_municipi.id = cups.id_municipi
LEFT JOIN
	res_country_state AS provincia ON provincia.id = res_municipi.state
LEFT JOIN
	giscedata_polissa_modcontractual AS mod ON mod.polissa_id = pol.id AND mod.modcontractual_ant IS NULL
LEFT JOIN
	giscedata_polissa_tarifa AS tar ON (
		(mod.id IS     NULL AND tar.id = pol.tarifa) OR
		(mod.id IS NOT NULL AND tar.id = mod.tarifa) OR
		FALSE
		)
LEFT JOIN (
	VALUES
		(10000,1000000000, '1'),
		(450,10000, '2'),
		(50,450, '3'),
		(15,50, '4'),
		(0,15, '5')
	) AS potencia(minim, maxim, tipopunto) ON (
		(mod.id IS     NULL AND potencia.minim < pol.potencia AND potencia.maxim >= pol.potencia) OR
		(mod.id IS NOT NULL AND potencia.minim < mod.potencia AND potencia.maxim >= mod.potencia) OR
		FALSE
	)
GROUP BY
	dist.id,
	dist.ref,
	dist.name,
	provincia.code,
	provincia.name,
	tar.name,
	tipocambio,
	tipopunto,
	TRUE
ORDER BY
	dist.name,
	provincia.code,
	tar.name,
	tipocambio,
	TRUE
;


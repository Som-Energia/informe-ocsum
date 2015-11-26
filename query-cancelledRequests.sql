/*
	All the requests cancelled during the given period.
	Implemented as:
	- c1_09 (change cancellation response)
	- c2_09 (change cancellation response)
	- a3_07 (direct cancellation response)
	- With a cn_09 or a3_07 step (cancellation response) created during the period
	- having the 'rebuig' flag off
	TODO: En criterio de conteo se dice que se usara FechaSolicitud del 09
*/
SELECT
	dist.id AS distriid,
	dist.ref2 AS refdistribuidora,
	provincia.code AS codiprovincia,
	tar.name AS tarname,
	tipocambio,
	tipopunto,
	COUNT(*) AS nreq,
	provincia.name AS nomprovincia,
	dist.name AS distriname,
	STRING_AGG(sw.id::text, ',' ORDER BY sw.id) AS casos
FROM
	(
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		'c1' AS process
	FROM giscedata_switching_c1_09
	WHERE
		data_acceptacio >= %(periodStart)s AND
		data_acceptacio < %(periodEnd)s AND
		rebuig = FALSE AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		'c2' AS process
	FROM giscedata_switching_c2_09
	WHERE
		data_acceptacio >= %(periodStart)s AND
		data_acceptacio < %(periodEnd)s AND
		rebuig = FALSE AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C4' AS tipocambio,
		'a3' AS process
	FROM giscedata_switching_a3_07
	WHERE
		data_acceptacio >= %(periodStart)s AND
		data_acceptacio < %(periodEnd)s AND
		rebuig = FALSE AND
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
		(mod.id IS     NULL AND tar.id = pol.tarifa) OR  /* No modification, use the on in the polissa */
		(mod.id IS NOT NULL AND tar.id = mod.tarifa) OR  /* Modification, means sometime activated, use the first value */
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
	tipopunto,
	TRUE
;


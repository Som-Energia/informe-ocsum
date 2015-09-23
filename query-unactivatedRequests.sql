/*
	All Requests sent during or before the period not answered during the period
*/
SELECT
	distri,
	refdistribuidora,
	codiprovincia,
	tarname,
	tipocambio,
	tipopunto,
	COUNT(*) AS nprocessos,
	SUM(CASE WHEN (
		%(periodEnd)s <= termini
		) THEN 1 ELSE 0 END) AS ontime,
	SUM(CASE WHEN (
		%(periodEnd)s > termini AND
		%(periodEnd)s <= termini + interval '15 days'
		) THEN 1 ELSE 0 END) AS late,
	SUM(CASE WHEN (
		%(periodEnd)s > termini + interval '15 days'
		) THEN 1 ELSE 0 END) AS verylate,
/*	SUM(CASE WHEN (
		%(periodEnd)s > termini + interval '90 days'
		) THEN 1 ELSE 0 END) AS unattended,
*/

	/* Not required by the report */
	SUM(CASE WHEN (
		%(periodEnd)s <= termini
		) THEN DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END
	) AS ontimeaddedtime,
	SUM(CASE WHEN (
		%(periodEnd)s > termini AND
		%(periodEnd)s <= termini + interval '15 days'
		) THEN DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END
	) AS lateaddedtime,
	SUM(CASE WHEN (
		%(periodEnd)s > termini + interval '15 days'
		) THEN DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END
	) AS verylateaddedtime,
	0 AS ontimeissues,
	0 AS lateissues,
	0 AS verylateissues,
	nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) as casos,
	TRUE
FROM (
	SELECT
		CASE
			WHEN cn02.id IS NOT NULL THEN cn05.data_activacio
			WHEN case_.priority = '5' THEN %(periodEnd)s
			ELSE NULL
		END AS data_activacio,
		sw.id AS sw_id,
		provincia.code AS codiprovincia,
		provincia.name AS nomprovincia,
		dist.id AS distri,
		dist.ref AS refdistribuidora,
		dist.name AS nomdistribuidora,
		tar.name AS tarname,
		cn02.tipocambio AS tipocambio,
		potencia.tipopunto as tipopunto,
		sw.create_date AS create_date,
		CASE
			WHEN tar.tipus = 'AT' THEN
				sw.create_date + interval '15 days'
			ELSE
				sw.create_date + interval '7 days'
		END AS termini,
		cn02.tipus,
		TRUE
	FROM
		( 
		SELECT
			id,
			header_id,
			data_acceptacio,
			rebuig,
			'C3' AS tipocambio,
			'c1' AS tipus
			FROM giscedata_switching_c1_02
		UNION
		SELECT
			id,
			header_id,
			data_acceptacio,
			rebuig,
			'C3' AS tipocambio,
			'c2' AS tipus
			FROM giscedata_switching_c2_02
		UNION
		SELECT
			id,
			header_id,
			data_acceptacio,
			rebuig,
			'C4' AS tipocambio,
			'a3' AS tipus
			FROM giscedata_switching_a3_02
		) AS cn02
	LEFT JOIN 
		giscedata_switching_step_header AS sth02 ON sth02.id = cn02.header_id
	LEFT JOIN 
		giscedata_switching AS sw ON sw.id = sth02.sw_id
	LEFT JOIN 
	(
		SELECT st05.id, header_id, data_activacio, sw_id
		FROM 
		( 
			SELECT id, header_id, data_activacio FROM giscedata_switching_c1_05
			UNION
			SELECT id, header_id, data_activacio FROM giscedata_switching_c2_05
			UNION
			SELECT id, header_id, data_activacio FROM giscedata_switching_a3_05
			UNION
			SELECT id, header_id, data_activacio FROM giscedata_switching_c2_07
		) AS st05
		JOIN
			giscedata_switching_step_header AS sth05 ON st05.header_id = sth05.id
	) AS cn05 ON cn05.sw_id = sw.id
	LEFT JOIN
		crm_case AS case_ ON case_.id = sw.case_id
	LEFT JOIN
		giscedata_cups_ps AS cups ON sw.cups_id = cups.id
	LEFT JOIN
		res_municipi ON  cups.id_municipi = res_municipi.id
	LEFT JOIN
		res_country_state AS provincia ON res_municipi.state = provincia.id
	LEFT JOIN
		giscedata_polissa AS pol ON cups_polissa_id = pol.id
	LEFT JOIN
		res_partner AS dist ON pol.distribuidora = dist.id
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
	WHERE
		(
			(
				/* Accepted prior to end of the period */
				cn02.id IS NOT NULL AND
				cn02.data_acceptacio <= %(periodEnd)s AND
				cn02.rebuig = FALSE AND
				( /* Not yet activated at the end of the period */
					cn05.id IS NULL OR
					cn05.data_activacio > %(periodEnd)s OR
					FALSE
				)
			) OR
			(
				/* No son de petites marcades com a aceptades sense 02 */
				cn02.id IS NULL AND
				case_.priority = '5' AND
				(
					pol.data_alta IS NULL OR
					pol.data_alta>%(periodEnd)s OR
					FALSE
				) AND
				sw.data_sollicitud<=%(periodEnd)s AND
				TRUE
			)
		)
	) AS s
GROUP BY
	s.nomdistribuidora,
	s.distri,
	s.refdistribuidora,
	s.tarname,
	tipocambio,
	tipopunto,
	s.codiprovincia,
	s.nomprovincia,
	TRUE
ORDER BY
	s.distri,
	s.codiprovincia,
	s.tarname,
	s.tipocambio,
	s.tipopunto,
	TRUE
;


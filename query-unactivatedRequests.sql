
SELECT
	COUNT(*) AS nprocessos,
	SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
	SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
	SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate,
/*	SUM(CASE WHEN (%(periodEnd)s > termini + interval '90 days') THEN 1 ELSE 0 END) AS unattended, */
	SUM(CASE WHEN (%(periodEnd)s <= termini) THEN
		DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END) AS ontimeaddedtime,
	SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN
		DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END) AS lateaddedtime,
	SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN
		DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END) AS verylateaddedtime, 
	codiprovincia,
	s.distri,
	s.tarname,
	s.refdistribuidora,
	nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) as casos,
	TRUE
FROM (
	SELECT DISTINCT
		CASE
			WHEN cn02.id IS NOT NULL THEN cn05.data_activacio
			WHEN case_.priority = '5' THEN %(periodEnd)s
			ELSE null
		END as data_activacio,
		sw.id AS sw_id,
		provincia.code AS codiprovincia,
		provincia.name AS nomprovincia,
		dist.id AS distri,
		dist.ref AS refdistribuidora,
		dist.name AS nomdistribuidora,
		tar.name AS tarname,
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
		giscedata_switching AS sw
	LEFT JOIN 
		giscedata_switching_step_header AS sth02 ON sth02.sw_id = sw.id
	JOIN ( 
		SELECT
			id,
			header_id,
			data_acceptacio,
			rebuig,
			'c1' AS tipus
			FROM giscedata_switching_c1_02
		UNION
		SELECT
			id,
			header_id,
			data_acceptacio,
			rebuig,
			'c2' AS tipus
			FROM giscedata_switching_c2_02
		) AS cn02 ON cn02.header_id = sth02.id
	LEFT JOIN 
		giscedata_switching_step_header AS sth05 ON sth05.sw_id = sw.id
	LEFT JOIN ( 
		SELECT id, header_id, data_activacio FROM giscedata_switching_c1_05
		UNION
		SELECT id, header_id, data_activacio FROM giscedata_switching_c2_05
		) AS cn05 ON cn05.header_id = sth05.id
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
		giscedata_polissa_tarifa AS tar ON pol.tarifa = tar.id
	WHERE
(
			(
				/* Accepted prior to end of the period */
				cn02.id IS NOT NULL AND
				cn02.data_acceptacio <= %(periodEnd)s AND
				NOT cn02.rebuig AND
				( /* Not yet activated at the end of the period */
					cn05.id IS NULL OR
					cn05.data_activacio > %(periodEnd)s OR
					FALSE
				)
			) OR
			( /* No son de petites marcades com a aceptades sense 02 */
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
	s.codiprovincia,
	s.nomprovincia,
	TRUE
ORDER BY
	s.distri,
	s.codiprovincia,
	s.tarname,
	TRUE
;


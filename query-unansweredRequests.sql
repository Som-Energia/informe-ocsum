/*
	Request sent previous to the end of the period and not answered (not accepted/rejected) during the period
	- c1_01 with no c1_02
	- c2_01 with no c2_02
	- a3_01 with no a3_02
	And case not closed at the end of the period
	TODO: Include case_.priority=5 with pol.data_alta within period
*/
SELECT
	s.distri,
	s.refdistribuidora,
	codiprovincia,
	s.tarname,
	s.tipocambio AS tipocambio,
	'5' AS tipopunto,
	COUNT(*) AS nprocessos,
	SUM(CASE WHEN (
		data_resposta <= termini
		) THEN 1 ELSE 0 END) AS ontime,
	SUM(CASE WHEN (
		data_resposta > termini AND
		data_resposta <= termini + interval '15 days'
		) THEN 1 ELSE 0 END) AS late,
	SUM(CASE WHEN (
		data_resposta > termini + interval '15 days'
		) THEN 1 ELSE 0 END) AS verylate,
/*	SUM(CASE WHEN (
		%(periodEnd)s > termini + interval '90 days'
		) THEN 1 ELSE 0 END) AS unattended,
*/
/*
	SUM(CASE WHEN (
		data_resposta <= termini
		) THEN DATE_PART('day',  data_resposta - create_date) ELSE 0 END
	) AS ontimeaddedtime,
	SUM(CASE WHEN (
		data_resposta > termini  AND
		data_resposta <= termini + interval '15 days'
		) THEN DATE_PART('day', data_resposta - create_date) ELSE 0 END
	) AS lateaddedtime,
	SUM(CASE WHEN (
		data_resposta > termini + interval '15 days'
		) THEN DATE_PART('day', data_resposta - create_date) ELSE 0 END
	) AS verylateaddedtime,
*/
	nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) AS casos,
	TRUE
FROM (
	SELECT
		sw.id AS sw_id,
		provincia.code AS codiprovincia,
		provincia.name AS nomprovincia,
		dist.id AS distri,
		dist.ref AS refdistribuidora,
		dist.name AS nomdistribuidora,
		tar.name AS tarname,
		step.tipocambio,
		sw.create_date AS create_date,
		CASE
			WHEN tar.tipus = 'AT' THEN
				sw.create_date + interval '15 days'
			ELSE
				sw.create_date + interval '7 days'
		END AS termini,
		%(periodEnd)s::date AS data_resposta,
		TRUE
	FROM
	(
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		'c1' AS process
	FROM giscedata_switching_c1_01
	WHERE
		create_date < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C3' AS tipocambio,
		'c2' AS process
	FROM giscedata_switching_c2_01
	WHERE
		create_date < %(periodEnd)s AND
		TRUE
	UNION
	SELECT
		id AS pass_id,
		header_id,
		'C4' AS tipocambio,
		'a3' AS process
	FROM giscedata_switching_a3_01
	WHERE
		create_date < %(periodEnd)s AND
		TRUE
	) AS step
	JOIN
	(
		SELECT
			sw.id AS sw_id,
			MIN(sth.id) AS header_id
		FROM	
			giscedata_switching_step_header AS sth
		LEFT JOIN
			giscedata_switching AS sw
			ON sth.sw_id = sw.id
		LEFT JOIN
			crm_case AS case_
			ON case_.id = sw.case_id
		WHERE
			(
				case_.date_closed IS NULL OR
				case_.date_closed > %(periodEnd)s
			) AND
			sth.date_created <= %(periodEnd)s AND
			sw.proces_id IN (
				SELECT id
				FROM giscedata_switching_proces as p
				WHERE
					p.name = 'C1' OR
					p.name = 'C2' OR
					p.name = 'A3' OR
					FALSE
				) AND
			TRUE
		GROUP BY
			sw.id,
			case_.date_closed,
			sw.create_date,
			TRUE
		HAVING COUNT(sth.id) = 1
		ORDER BY sw.id
		) AS uniqstep
		ON uniqstep.header_id = step.header_id
	LEFT JOIN
		giscedata_switching AS sw ON sw.id = uniqstep.sw_id
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
			(mod.id IS NULL     AND tar.id = pol.tarifa) OR
			(mod.id IS NOT NULL AND tar.id = mod.tarifa) OR
			FALSE)
	WHERE
		/* No s'ha tancat el cas abans de finalitzar el periode */
		(
			case_.date_closed IS NULL OR
			case_.date_closed > %(periodEnd)s
		) AND

		/* No s'ha fet l'alta abans de finalitzar el periode */
		(
			pol.data_alta IS NULL OR
			pol.data_alta>%(periodEnd)s
		) AND

		TRUE
	) AS s
GROUP BY
	s.nomdistribuidora,
	s.distri,
	s.refdistribuidora,
	s.tarname,
	s.tipocambio,
	s.codiprovincia,
	s.nomprovincia,
	TRUE
ORDER BY
	s.distri,
	s.codiprovincia,
	s.tarname,
	TRUE
;


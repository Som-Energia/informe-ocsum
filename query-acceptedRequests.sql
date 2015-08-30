/*
	Request accepted during the period
	- c1_02
	- c2_02
	- a3_03
	- with rejected=FALSE
º	TODO: Include case_.priority=5 with pol.data_alta within period
*/
SELECT
	COUNT(*) AS nprocessos,
	SUM(CASE WHEN (
		data_acceptacio <= termini
		) THEN 1 ELSE 0 END) AS ontime,
	SUM(CASE WHEN (
		data_acceptacio > termini AND
		data_acceptacio <= termini + interval '15 days'
		) THEN 1 ELSE 0 END) AS late,
	SUM(CASE WHEN (
		data_acceptacio > termini + interval '15 days'
		) THEN 1 ELSE 0 END) AS verylate,
/*	SUM(CASE WHEN (
		%(periodEnd)s > termini + interval '90 days'
		) THEN 1 ELSE 0 END) AS unattended,
*/

	SUM(CASE WHEN (
		data_acceptacio <= termini
		) THEN DATE_PART('day',  data_acceptacio - create_date) ELSE 0 END
	) AS ontimeaddedtime,
	SUM(CASE WHEN (
		data_acceptacio > termini  AND
		data_acceptacio <= termini + interval '15 days'
		) THEN DATE_PART('day', data_acceptacio - create_date) ELSE 0 END
	) AS lateaddedtime,
	SUM(CASE WHEN (
		data_acceptacio > termini + interval '15 days'
		) THEN DATE_PART('day', data_acceptacio - create_date) ELSE 0 END
	) AS verylateaddedtime,

	codiprovincia,
	s.distri,
	s.tarname,
	s.refdistribuidora,
	nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) as casos,
	TRUE
FROM (
	SELECT
		data_acceptacio,
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
		TRUE
	FROM (
		SELECT
			id AS pass_id,
			header_id,
			'C3' AS tipo_cambio,
			'c1' AS process,
			data_acceptacio,
			TRUE
		FROM
			giscedata_switching_c1_02
		WHERE
			data_acceptacio >= %(periodStart)s AND
			data_acceptacio <= %(periodEnd)s AND
			NOT rebuig AND
			TRUE
		UNION
		SELECT
			id AS pass_id,
			header_id,
			'C3' as tipo_cambio,
			'c2' as process,
			data_acceptacio,
			TRUE
		FROM
			giscedata_switching_c2_02
		WHERE
			data_acceptacio >= %(periodStart)s AND
			data_acceptacio <= %(periodEnd)s AND
			NOT rebuig AND
			TRUE
/*
		UNION
		SELECT
			id AS pass_id,
			header_id,
			'C4' as tipo_cambio,
			'a3' as process,
			data_acceptacio,
			TRUE
		FROM
			giscedata_switching_a3_02
		WHERE
			data_acceptacio >= %(periodStart)s AND
			data_acceptacio <= %(periodEnd)s AND
			NOT rebuig AND
			TRUE
*/
	/* TODO:
	LEFT JOIN
		crm_case AS case_ ON case_.id = sw.case_id
		case_.priority=5 with pol.data_alta within the period
*/
		) AS step
	LEFT JOIN 
		giscedata_switching_step_header AS sth ON step.header_id = sth.id
	LEFT JOIN 
		giscedata_switching AS sw ON sw.id = sth.sw_id
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


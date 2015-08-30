/*
	Request accepted during the period
	- c1_02
	- c2_02
	- a3_03
	- with rejected=FALSE
*/
SELECT
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

	SUM(CASE WHEN (
		%(periodEnd)s <= termini
		) THEN DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END
	) AS ontimeaddedtime,
	SUM(CASE WHEN (
		(%(periodEnd)s > termini)  AND
		(%(periodEnd)s <= termini + interval '15 days')
		) THEN DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END
	) AS lateaddedtime,
	SUM(CASE WHEN (
		%(periodEnd)s > termini + interval '15 days'
		) THEN DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END
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
		CASE
			WHEN c202.id IS NOT NULL THEN c202.data_acceptacio
			WHEN c102.id IS NOT NULL THEN c102.data_acceptacio
			WHEN case_.priority = '5' THEN %(periodEnd)s
			ELSE null
		END as data_acceptacio,
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
	FROM
		giscedata_switching AS sw
	LEFT JOIN 
		giscedata_switching_step_header AS sth ON sth.sw_id = sw.id
	LEFT JOIN
		giscedata_switching_c1_02 AS c102 ON c102.header_id = sth.id
	LEFT JOIN
		giscedata_switching_c2_02 AS c202 ON c202.header_id = sth.id
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
			c102.id IS NOT NULL AND
			c102.data_acceptacio >= %(periodStart)s AND
			c102.data_acceptacio <= %(periodEnd)s AND
			NOT c102.rebuig AND
			TRUE
			
		) OR (
			c202.id IS NOT NULL AND
			c202.data_acceptacio >= %(periodStart)s AND
			c202.data_acceptacio <= %(periodEnd)s AND
			NOT c202.rebuig AND
			TRUE
		) OR (
			/* No son de petites marcades com a aceptades sense 02 */
			c202.data_acceptacio >= %(periodStart)s AND
			c202.data_acceptacio <= %(periodEnd)s AND
			pol.data_alta IS NOT NULL AND
			pol.data_alta>=%(periodStart)s AND
			pol.data_alta<=%(periodEnd)s AND
			case_.priority = '5' AND
			FALSE
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


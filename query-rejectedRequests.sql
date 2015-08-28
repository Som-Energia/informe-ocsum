
SELECT
	COUNT(*) AS nprocessos,
	SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
	SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
	SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate, 
/*	SUM(CASE WHEN (%(periodEnd)s > termini + interval '90 days') THEN 1 ELSE 0 END) AS unattended, */

	SUM(CASE WHEN (%(periodEnd)s <= termini) THEN
		DATE_PART('day', %(periodEnd)s - s.create_date) ELSE 0 END
		) AS ontimeaddedtime,
	SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN
		DATE_PART('day', %(periodEnd)s - s.create_date) ELSE 0 END
		) AS lateaddedtime,
	SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN
		DATE_PART('day', %(periodEnd)s - s.create_date) ELSE 0 END
		) AS verylateaddedtime,
	provincia.code AS codiprovincia,
	s.distri,
	s.rejectreason,
	s.tarname,
	s.refdistribuidora,
	provincia.name AS nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) as casos,
	TRUE
FROM (
	SELECT 
		steph.date_created as data_rebuig,
		sw.id AS sw_id,
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
		sw.cups_id,
		(
			SELECT MIN(motiu.name)
			FROM sw_step_header_rebuig_ref AS h2r 
			LEFT JOIN
				giscedata_switching_rebuig AS rebuig ON h2r.rebuig_id = rebuig.id
			LEFT JOIN
				giscedata_switching_motiu_rebuig AS motiu ON rebuig.motiu_rebuig = motiu.id
			WHERE h2r.header_id = steph.id
		) AS rejectreason,
		TRUE
	FROM
		giscedata_switching AS sw
	LEFT JOIN 
		giscedata_switching_step_header AS steph ON steph.sw_id = sw.id
	LEFT JOIN (
			SELECT *, 1 as process FROM giscedata_switching_c1_02
		UNION
			SELECT *, 2 as process FROM giscedata_switching_c2_02
		) AS pass ON pass.header_id = steph.id
	LEFT JOIN
		crm_case AS case_ ON case_.id = sw.case_id
	LEFT JOIN 
		giscedata_polissa AS pol ON cups_polissa_id = pol.id
	LEFT JOIN 
		res_partner AS dist ON pol.distribuidora = dist.id
	LEFT JOIN
		giscedata_polissa_tarifa AS tar ON pol.tarifa = tar.id
	WHERE
		/* Ens focalitzem en els processos indicats */
		sw.proces_id = ANY( %(process)s )  AND
		pass.id IS NOT NULL AND
		steph.date_created >= %(periodStart)s AND
		steph.date_created <= %(periodEnd)s AND
		pass.rebuig AND
		TRUE
	) as s 
LEFT JOIN
	giscedata_cups_ps AS cups ON s.cups_id = cups.id
LEFT JOIN
	res_municipi ON  cups.id_municipi = res_municipi.id
LEFT JOIN
	res_country_state AS provincia ON res_municipi.state = provincia.id
GROUP BY
	s.nomdistribuidora,
	s.distri,
	s.refdistribuidora,
	s.tarname,
	codiprovincia,
	nomprovincia,
	s.rejectreason,
	TRUE
ORDER BY
	s.distri,
	codiprovincia,
	s.tarname,
	TRUE
;


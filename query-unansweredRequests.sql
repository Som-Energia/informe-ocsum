/*
	Request not answered at the end of the period.
	May include requests sent during previous periods.
*/
SELECT
	s.distri,
	s.refdistribuidora,
	codiprovincia,
	s.tarname,
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
	nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) as casos,
	TRUE
FROM (
	SELECT
		count(sth.id) AS npassos,
		sw.id AS sw_id,
		provincia.code AS codiprovincia,
		provincia.name AS nomprovincia,
		dist.id AS distri,
		dist.ref AS refdistribuidora,
		dist.name AS nomdistribuidora,
		tar.name AS tarname,
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
		giscedata_polissa AS pol ON cups_polissa_id = pol.id
	LEFT JOIN 
		res_partner AS dist ON pol.distribuidora = dist.id
	LEFT JOIN
		giscedata_polissa_modcontractual AS mod ON mod.polissa_id = pol.id AND mod.modcontractual_ant IS NULL
	LEFT JOIN
		giscedata_polissa_tarifa AS tar ON ((mod.id IS NULL AND tar.id = pol.tarifa) OR (mod.id IS NOT NULL AND tar.id = mod.tarifa))
	LEFT JOIN
		giscedata_cups_ps AS cups ON sw.cups_id = cups.id
	LEFT JOIN
		res_municipi ON  cups.id_municipi = res_municipi.id
	LEFT JOIN
		res_country_state AS provincia ON res_municipi.state = provincia.id
	LEFT JOIN 
		giscedata_switching_step_header AS sth ON sth.sw_id = sw.id
	LEFT JOIN 
		giscedata_switching_proces AS pr ON sw.proces_id = pr.id
	LEFT JOIN
		crm_case AS case_ ON case_.id = sw.case_id
	WHERE
		/* S'ha creat el cas abans del final del periode */
		sth.date_created < %(periodEnd)s AND

		/* No s'ha tancat el cas abans de finalitzar el periode */
		(case_.date_closed IS NULL OR case_.date_closed >%(periodEnd)s ) AND

		/* No s'ha fet l'alta abans de finalitzar el periode */
		(pol.data_alta IS NULL OR pol.data_alta>%(periodEnd)s ) AND

		/* Ens focalitzem en els processos indicats */
		sw.proces_id = ANY( %(process)s )  AND

		/* No son de petites marcades com a rebutjades sense 02 */
		case_.priority != '4' AND

		/* No son de petites marcades com a aceptades sense 02 */
		case_.priority != '5' AND

		TRUE
	GROUP BY
		sw.id,
		refdistribuidora,
		tarname,
		tar.tipus,
		dist.id,
		codiprovincia,
		nomprovincia,
		TRUE
	ORDER BY
		sw.id,
		tarname,
		dist.id,
		codiprovincia,
		nomprovincia,
		TRUE
	) AS s
LEFT JOIN 
	giscedata_switching_step_header AS sth ON sth.sw_id = s.sw_id
LEFT JOIN
	giscedata_switching_c1_01 AS c101 ON c101.header_id = sth.id
LEFT JOIN
	giscedata_switching_c2_01 AS c201 ON c201.header_id = sth.id
WHERE
	s.npassos = 1 AND
	NOT (
		c101.id IS NULL AND
		c201.id IS NULL
	)
GROUP BY
	s.nomdistribuidora,
	s.distri,
	s.npassos,
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


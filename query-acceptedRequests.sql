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
		/* c1_02, rebuig=false, accepted within the period */
			SELECT
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
		/* c2_02, rebuig=false, accepted within the period */
			SELECT
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
		/* a3_02, rebuig=false, accepted within the period */
/*
			UNION
			SELECT
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
		UNION
		/* single 01 step, priority=5, polissa.data_alta en el periode */
			SELECT
				sth.id AS header_id,
				step01.tipo_cambio AS tipo_cambio,
				step01.process AS process,
				pol.data_alta AS data_acceptacio,
				TRUE
			FROM
				crm_case AS case_
			LEFT JOIN
				giscedata_switching AS sw ON sw.case_id = case_.id
			LEFT JOIN
				giscedata_switching_step_header AS sth ON sth.sw_id = sw.id
			LEFT JOIN
				giscedata_polissa AS pol ON pol.id = sw.cups_polissa_id
			JOIN (
				/* single step cases */
				SELECT
					sw.id,
					count(sth.id) AS nsteps
				FROM	
					giscedata_switching AS sw
				LEFT JOIN
					giscedata_switching_step_header AS sth
					ON sth.sw_id = sw.id
				GROUP BY sw.id
				HAVING count(sth.id) = 1
			) AS swunic ON sw.id = swunic.id
			JOIN (
				/* and 01 steps */
				SELECT
					'C3' AS tipo_cambio,
					'c1' AS process,
					st.header_id as header_id
				FROM
					giscedata_switching_c1_01 AS st
				UNION
				SELECT
					'C3' AS tipo_cambio,
					'c2' AS process,
					st.header_id as header_id
				FROM
					giscedata_switching_c2_01 AS st
				UNION
				SELECT
					'C4' AS tipo_cambio,
					'a3' AS process,
					st.header_id as header_id
				FROM
					giscedata_switching_a3_01 AS st

				) AS step01 ON step01.header_id = sth.id
			WHERE
				pol.data_alta >= %(periodStart)s AND
				pol.data_alta <= %(periodEnd)s AND
				case_.priority='5' AND
				TRUE
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


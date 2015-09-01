/*
	Request accepted during the period
	- c1_02
	- c2_02
	- a3_02
	- with rejected=TRUE
º	TODO: Include case_.priority=5 with pol.data_alta within period
*/
SELECT
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

	codiprovincia,
	s.distri,
	s.rejectreason,
	s.tarname,
	s.refdistribuidora,
	nomprovincia,
	s.nomdistribuidora,
	STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id) AS casos,
	TRUE
FROM (
	SELECT
		data_resposta,
		sw.id AS sw_id,
		provincia.code AS codiprovincia,
		provincia.name AS nomprovincia,
		dist.id AS distri,
		dist.ref AS refdistribuidora,
		dist.name AS nomdistribuidora,
		tar.name AS tarname,
		sw.create_date AS create_date,
		(
			SELECT MIN(motiu.name)
			FROM sw_step_header_rebuig_ref AS h2r 
			LEFT JOIN
				giscedata_switching_rebuig AS rebuig ON h2r.rebuig_id = rebuig.id
			LEFT JOIN
				giscedata_switching_motiu_rebuig AS motiu ON rebuig.motiu_rebuig = motiu.id
			WHERE h2r.header_id = sth.id
		) AS rejectreason,
		CASE
			WHEN tar.tipus = 'AT' THEN
				sw.create_date + interval '15 days'
			ELSE
				sw.create_date + interval '7 days'
		END AS termini,
		TRUE
	FROM (
		/* c1_02, rebuig=true, accepted within the period */
			SELECT
				header_id,
				'C3' AS tipo_cambio,
				'c1' AS process,
				sth.date_created AS data_resposta,
				TRUE
			FROM
				giscedata_switching_c1_02 AS step
			LEFT JOIN
				giscedata_switching_step_header AS sth ON step.header_id = sth.id
			WHERE
				sth.date_created >= %(periodStart)s AND
				sth.date_created <= %(periodEnd)s AND
				rebuig AND
				TRUE
		UNION
		/* c2_02, rebuig=true, accepted within the period */
			SELECT
				step.header_id,
				'C3' AS tipo_cambio,
				'c2' AS process,
				sth.date_created AS data_resposta,
				TRUE
			FROM
				giscedata_switching_c2_02 AS step
			LEFT JOIN
				giscedata_switching_step_header AS sth ON step.header_id = sth.id
			WHERE
				sth.date_created >= %(periodStart)s AND
				sth.date_created <= %(periodEnd)s AND
				rebuig AND
				TRUE
		UNION
		/* a3_02, rebuig=true, accepted within the period */
			SELECT
				header_id,
				'C4' AS tipo_cambio,
				'a3' AS process,
				sth.date_created AS data_resposta,
				TRUE
			FROM
				giscedata_switching_a3_02 AS step
			LEFT JOIN
				giscedata_switching_step_header AS sth ON step.header_id = sth.id
			WHERE
				sth.date_created >= %(periodStart)s AND
				sth.date_created <= %(periodEnd)s AND
				rebuig AND
				TRUE
		UNION
		/* single 01 step, priority=5, polissa.data_alta en el periode */
			SELECT
				sth.id AS header_id,
				step01.tipo_cambio AS tipo_cambio,
				step01.process AS process,
				pol.data_alta AS data_resposta,
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
					st.header_id AS header_id
				FROM
					giscedata_switching_c1_01 AS st
				UNION
				SELECT
					'C3' AS tipo_cambio,
					'c2' AS process,
					st.header_id AS header_id
				FROM
					giscedata_switching_c2_01 AS st
				UNION
				SELECT
					'C4' AS tipo_cambio,
					'a3' AS process,
					st.header_id AS header_id
				FROM
					giscedata_switching_a3_01 AS st

				) AS step01 ON step01.header_id = sth.id
			WHERE
				pol.data_alta >= %(periodStart)s AND
				pol.data_alta <= %(periodEnd)s AND
				case_.priority='4' AND
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
		giscedata_polissa_modcontractual AS mod ON mod.polissa_id = pol.id AND mod.modcontractual_ant IS NULL
	LEFT JOIN
		giscedata_polissa_tarifa AS tar ON ((mod.id IS NULL AND tar.id = pol.tarifa) OR (mod.id IS NOT NULL AND tar.id = mod.tarifa))
	) AS s
GROUP BY
	s.nomdistribuidora,
	s.distri,
	s.refdistribuidora,
	s.tarname,
	s.codiprovincia,
	s.nomprovincia,
	s.rejectreason,
	TRUE
ORDER BY
	s.distri,
	s.codiprovincia,
	s.tarname,
	TRUE
;


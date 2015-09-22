/*
	Request accepted during the period
	- c1_02
	- c2_02
	- a3_03
	- with rejected=FALSE
	TODO: Include case_.priority=5 with pol.data_alta within period
*/
SELECT
	s.distri,
	s.refdistribuidora,
	codiprovincia,
	s.tarname,
	s.tipocambio,
	s.tipopunto AS tipopunto,
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
		step.tipocambio AS tipocambio,
        potencia.tipopunto as tipopunto,
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
				'C3' AS tipocambio,
				'c1' AS process,
				data_acceptacio AS data_resposta,
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
				'C3' AS tipocambio,
				'c2' AS process,
				data_acceptacio AS data_resposta,
				TRUE
			FROM
				giscedata_switching_c2_02
			WHERE
				data_acceptacio >= %(periodStart)s AND
				data_acceptacio <= %(periodEnd)s AND
				NOT rebuig AND
				TRUE
		UNION
		/* a3_02, rebuig=false, accepted within the period */
			SELECT
				header_id,
				'C4' AS tipocambio,
				'a3' AS process,
				data_acceptacio AS data_resposta,
				TRUE
			FROM
				giscedata_switching_a3_02
			WHERE
				data_acceptacio >= %(periodStart)s AND
				data_acceptacio <= %(periodEnd)s AND
				NOT rebuig AND
				TRUE
		UNION
		/* single 01 step, priority=5, polissa.data_alta en el periode */
			SELECT
				sth.id AS header_id,
				step01.tipocambio AS tipocambio,
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
					'C3' AS tipocambio,
					'c1' AS process,
					st.header_id AS header_id
				FROM
					giscedata_switching_c1_01 AS st
				UNION
				SELECT
					'C3' AS tipocambio,
					'c2' AS process,
					st.header_id AS header_id
				FROM
					giscedata_switching_c2_01 AS st
				UNION
				SELECT
					'C4' AS tipocambio,
					'a3' AS process,
					st.header_id AS header_id
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
        
	) AS s
GROUP BY
	s.nomdistribuidora,
	s.distri,
	s.refdistribuidora,
	s.tarname,
	s.codiprovincia,
	s.nomprovincia,
	s.tipocambio,
	s.tipopunto,
	TRUE
ORDER BY
	s.distri,
	s.codiprovincia,
	s.tarname,
	s.tipocambio,
	s.tipopunto,
	TRUE
;


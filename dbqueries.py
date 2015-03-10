#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import psycopg2
import psycopg2.extras
import datetime

from dbutils import *

def idsProcessos(db):
	with db.cursor() as cur:
		cur.execute("""
			SELECT
				pr.name,
				pr.id
			FROM
				giscedata_switching_proces as pr
			""")
		return dict( (row.name, row.id) for row in fetchNs(cur))

def idsPasses(db, *args):
	with db.cursor() as cur :
		cur.execute("""
			SELECT
				st.id as id,
				st.name as name,
				pr.name as process
			FROM
				giscedata_switching_step as st
			LEFT JOIN
				giscedata_switching_proces as pr ON pr.id = st.proces_id
			"""+ (
			"WHERE pr.name IN %(process)s " if args else ""
			),
			dict(
				process=args
				)
			)
		passes = dict( (row.process+'_'+row.name, row.id) for row in fetchNs(cur))
		passesNames = dict((value, key) for key,value in passes.items())
		return passes

import unittest

@unittest.skipIf(config is None, "No dbconfig.py file found")
class Test_switching(unittest.TestCase):

	def setUp(self):
		from dbconfig import psycopg as config
		self.db = psycopg2.connect(**config)

	def tearDown(self):
		self.db.rollback()
		self.db.close()

	def test_idsProcessos(self):
		processos = idsProcessos(self.db)
		self.assertEqual(
			sorted(processos.keys()),[
				'A3', 'B1', 'C1', 'C2', 'D1', 'M1'
			])

	def test_idsPasses(self):
		passes = idsPasses(self.db)
		self.assertEqual(
			sorted(passes.keys()), [
				'A3_01', 'A3_02', 'A3_03', 'A3_04', 'A3_05', 'A3_06', 'A3_07',
				'B1_01', 'B1_02', 'B1_03', 'B1_04', 'B1_05', 
				'C1_01', 'C1_02', 'C1_05', 'C1_06', 'C1_08', 'C1_09', 'C1_10', 'C1_11',
				'C2_01', 'C2_02', 'C2_03', 'C2_04', 'C2_05', 'C2_06', 'C2_07', 'C2_08', 'C2_09', 'C2_10', 'C2_11', 'C2_12',
				'D1_01',
				'M1_01', 'M1_02', 'M1_03', 'M1_04', 'M1_05', 'M1_06', 'M1_07', 'M1_08',
			])

	def test_idsPasses_filteringByProcess(self):
		passes = idsPasses(self.db, 'B1', 'C2')
		self.assertEqual(
			sorted(passes.keys()), [
				'B1_01', 'B1_02', 'B1_03', 'B1_04', 'B1_05', 
				'C2_01', 'C2_02', 'C2_03', 'C2_04', 'C2_05', 'C2_06', 'C2_07', 'C2_08', 'C2_09', 'C2_10', 'C2_11', 'C2_12',
			])

# TODO: Not covered
def numeroDeCasos(db) :
	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				pr.name,
				count(sw.id) AS npassos
			FROM
				giscedata_switching AS sw
			LEFT JOIN
				giscedata_switching_proces AS pr ON pr.id = sw.proces_id
			GROUP BY
				sw.proces_id,
				pr.name
			""")
		return dict((name, count) for name, count in cur)


def peticionsPendentsDeResposta(db, inici, final, cursorManager=nsList):

	processos = idsProcessos(db)

	# TODO: S'esta fent servir incorrectament la creacio del cas com a data de carga del fitxer al sistema de la distribuidora
	# TODO: Group by 'TipoCambio' (siempre C3? cambio comercializadora. Cuando C4?)
	# TODO: Group by 'TipoPunto'
	# TODO: Group by 'Comer_saliente' (siempre 0 - Desconocido?)
	# TODO: Group by 'Comer_entrante' (siempre 1 - Somenergia?)
	# TODO: Revisar interval de les dates
	# TODO: Plaç depenent de la tarifa
	# TODO: Tarifa s'agafa de l'actual, i pot haver canviat
	# TODO: La prioritat (que es fa servir per indicar gestionades sense 02) podria ser posterior a la data

	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				COUNT(*) AS nprocessos,
				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate, 
/*				SUM(CASE WHEN (%(periodEnd)s > termini + interval '90 days') THEN 1 ELSE 0 END) AS unattended, */
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
					giscedata_polissa_tarifa AS tar ON pol.tarifa = tar.id
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
			""",
			dict(
				process = [processos[name] for name in 'C1','C2'],
				periodEnd = final,
				tarifesAltaTensio = [
					'3.1A',
				],
			))
		result = cursorManager(cur)
		return result

def peticionsAcceptades(db, inici, final, cursorManager=nsList):

	# TODO:
	# - Date on 5-priority cases (accepted without a 02 step) migth not be real and outside the period.

	processos = idsProcessos(db)

	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				COUNT(*) AS nprocessos,
				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate, 
/*				SUM(CASE WHEN (%(periodEnd)s > termini + interval '90 days') THEN 1 ELSE 0 END) AS unattended, */

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
				SELECT
					CASE
						WHEN c202.id IS NOT NULL THEN c202.data_acceptacio
						WHEN c102.id IS NOT NULL THEN c102.data_acceptacio
						WHEN case_.priority = '5' THEN %(periodEnd)s
						ELSE null
					END as data_accpetacio,
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
						case_.priority = '5'
					)
				) as s 
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
			"""
			,dict(

				process = [processos[name] for name in 'C1','C2'],
				periodStart = inici,
				periodEnd = final,
			))
		result = cursorManager(cur)
		return result

def rejectedRequests(db, inici, final, cursorManager=nsList):

	# TODO:
	# - Date on 5-priority cases (accepted without a 02 step) migth not be real and outside the period.

	processos = idsProcessos(db)

	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				COUNT(*) AS nprocessos,
				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate, 
/*				SUM(CASE WHEN (%(periodEnd)s > termini + interval '90 days') THEN 1 ELSE 0 END) AS unattended, */

				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN
					DATE_PART('day', %(periodEnd)s - s.create_date) ELSE 0 END) AS ontimeaddedtime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN
					DATE_PART('day', %(periodEnd)s - s.create_date) ELSE 0 END) AS lateaddedtime,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN
					DATE_PART('day', %(periodEnd)s - s.create_date) ELSE 0 END) AS verylateaddedtime, 
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
			"""
			,dict(

				process = [processos[name] for name in 'C1','C2'],
				periodStart = inici,
				periodEnd = final,
			))
		result = cursorManager(cur)
		return result


def activatedRequests(db, inici, final, cursorManager=nsList):
	processos = idsProcessos(db)

	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				count(*),
				step.data_activacio,
				STRING_AGG(sw.id::text, ',' ORDER BY sw.id) as casos
			FROM
				(
				SELECT
					id as pass_id,
					header_id,
					"tarifaATR",
					data_activacio,
					contracte_atr,
					1 as process
				FROM giscedata_switching_c1_05
				WHERE
					data_activacio >= %(periodStart)s AND
					data_activacio < %(periodEnd)s AND
					TRUE
				UNION
				SELECT
					id as pass_id,
					header_id,
					"tarifaATR",
					data_activacio,
					contracte_atr,
					2 as process
				FROM giscedata_switching_c2_05
				WHERE
					data_activacio >= %(periodStart)s AND
					data_activacio < %(periodEnd)s AND
					TRUE
				) AS step
			LEFT JOIN
				giscedata_switching_step_header AS sth ON step.header_id = sth.id
			LEFT JOIN
				giscedata_switching AS sw ON sw.id = sth.sw_id
			LEFT JOIN
				giscedata_cups_ps AS cups ON cups.id = sw.cups_id
			LEFT JOIN
				giscedata_polissa AS pol ON pol.id = sw.cups_polissa_id
			LEFT JOIN
				res_partner AS dist ON dist.id = pol.distribuidora
			LEFT JOIN
				giscedata_polissa_tarifa AS tar ON tar.id = pol.tarifa
			LEFT JOIN
				res_municipi ON res_municipi.id = cups.id_municipi
			LEFT JOIN
				res_country_state AS provincia ON provincia.id = res_municipi.state
/*
				LEFT JOIN
					giscedata_switching_proces AS pr ON sw.proces_id = pr.id
				LEFT JOIN
					crm_case AS case_ ON case_.id = sw.case_id
*/
			WHERE
				TRUE
			GROUP BY
				step.data_activacio,
				tar.name,
				dist.name,
				TRUE
			ORDER BY
				step.data_activacio,
				tar.name,
				dist.name,
				TRUE
			"""
			,dict(
				process = [processos[name] for name in 'C1','C2'],
				periodStart = inici,
				periodEnd = final,
			))

		result = cursorManager(cur)
	return result


import b2btest

@unittest.skipIf(config is None, "No dbconfig.py file found")
class OcsumReport_Test(b2btest.TestCase) :

	def setUp(self):
		from dbconfig import psycopg as config
		self.db = psycopg2.connect(**config)

	def tearDown(self):
		self.db.rollback()
		self.db.close()

	def _test_peticionsPendentsDeResposta(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = peticionsPendentsDeResposta(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'peticionsPendentsDeResposta-{}.csv'.format(inici))

	def test_peticionsPendentsDeResposta_2014_02(self) :
		self._test_peticionsPendentsDeResposta((2014,2))

	def test_peticionsPendentsDeResposta_2014_03(self) :
		self._test_peticionsPendentsDeResposta((2014,3))

	def test_peticionsPendentsDeResposta_2014_04(self) :
		self._test_peticionsPendentsDeResposta((2014,4))

	def test_peticionsPendentsDeResposta_2014_12(self) :
		self._test_peticionsPendentsDeResposta((2014,12))

	def test_peticionsPendentsDeResposta_2015_02(self) :
		self._test_peticionsPendentsDeResposta((2015,2))


	def _test_peticionsAcceptades(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = peticionsAcceptades(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'peticionsAcceptades-{}.csv'.format(inici))

	def test_peticionsAcceptades_2014_02(self) :
		self._test_peticionsAcceptades((2014,2))

	def test_peticionsAcceptades_2014_03(self) :
		self._test_peticionsAcceptades((2014,3))


	def _test_rejectedRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = rejectedRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'rejectedRequests-{}.csv'.format(inici))

	def test_rejectedRequests_2014_02(self) :
		self._test_rejectedRequests((2014,2))


	def _test_activatedRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = activatedRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'activatedRequests-{}.csv'.format(inici))

	def test_activatedRequests_2014_02(self) :
		self._test_activatedRequests((2014,2))








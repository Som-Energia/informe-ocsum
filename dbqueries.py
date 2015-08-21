#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import psycopg2
import psycopg2.extras
import datetime
from dbutils import *
import os

def loadQuery(name):
	scriptPath=os.path.abspath(os.path.dirname(__file__))
	fullFilename=os.path.join(scriptPath, name)
	with open(fullFilename) as queryFile:
		return queryFile.read()

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


def unansweredRequests(db, inici, final, cursorManager=nsList):

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
		cur.execute(
			loadQuery('query-unansweredRequests.sql'),
			dict(
				process = [processos[name] for name in 'C1','C2'],
				periodEnd = final,
				tarifesAltaTensio = [
					'3.1A',
				],
			))
		result = cursorManager(cur)
		return result

def acceptedRequests(db, inici, final, cursorManager=nsList):

	# TODO:
	# - Date on 5-priority cases (accepted without a 02 step) migth not be real and outside the period.

	processos = idsProcessos(db)

	with db.cursor() as cur :
		cur.execute(
			loadQuery('query-acceptedRequests.sql'),
			dict(

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
		cur.execute(
			loadQuery('query-rejectedRequests.sql'),
			dict(

				process = [processos[name] for name in 'C1','C2'],
				periodStart = inici,
				periodEnd = final,
			))
		result = cursorManager(cur)
		return result


def activatedRequests(db, inici, final, cursorManager=nsList):
	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				COUNT(*),
				SUM(CASE WHEN (
					data_activacio <= sw.create_date + interval '66 days'
					) THEN 1 ELSE 0 END) AS ontime,
				SUM(CASE WHEN (
					data_activacio >  sw.create_date + interval '66 days' AND 
					data_activacio <= sw.create_date + interval '81 days'
					) THEN 1 ELSE 0 END) AS late,
				SUM(CASE WHEN (
					data_activacio >  sw.create_date + interval '81 days'
					) THEN 1 ELSE 0 END) AS verylate,
				SUM(CASE WHEN (
					data_activacio <= sw.create_date + interval '66 days'
					) THEN DATE_PART('day', data_activacio - sw.create_date ) ELSE 0 END
				) AS ontimeaddedtime,
				SUM(CASE WHEN (
					data_activacio >  sw.create_date + interval '66 days' AND 
					data_activacio <= sw.create_date + interval '81 days'
					) THEN DATE_PART('day', data_activacio - sw.create_date ) ELSE 0 END
				) AS lateaddedtime,
				SUM(CASE WHEN (
					data_activacio >  sw.create_date + interval '81 days'
					) THEN DATE_PART('day', data_activacio - sw.create_date ) ELSE 0 END
				) AS verylateaddedtime,
				0 AS ontimeissues,
				0 AS lateissues,
				0 AS verylateissues,
				dist.id AS distriid,
				dist.ref AS refdistribuidora,
				tar.name as tarname,
				provincia.code AS codiprovincia,
				provincia.name AS nomprovincia,
				dist.name AS distriname,
				STRING_AGG(sw.id::text, ',' ORDER BY sw.id) AS casos
			FROM
				(
				SELECT
					id AS pass_id,
					header_id,
					data_activacio,
					1 AS process
				FROM giscedata_switching_c1_05
				WHERE
					data_activacio >= %(periodStart)s AND
					data_activacio < %(periodEnd)s AND
					TRUE
				UNION
				SELECT
					id AS pass_id,
					header_id,
					data_activacio,
					2 AS process
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
			GROUP BY
				tar.name,
				dist.name,
				provincia.code,
				dist.id,
				dist.ref,
				provincia.name,
				dist.name,
				TRUE
			ORDER BY
				tar.name,
				dist.name,
				provincia.code,
				TRUE
			"""
			,dict(
				periodStart = inici,
				periodEnd = final,
			))

		result = cursorManager(cur)
	return result

def sentRequests(db, inici, final, cursorManager=nsList) :
	with db.cursor() as cur :
		cur.execute(
			loadQuery('query-sentRequests.sql'),
			dict(
				periodStart = inici,
				periodEnd = final,
			))
		result = cursorManager(cur)
	return result


def unactivatedRequests(db, inici, final, cursorManager=nsList):

	processos = idsProcessos(db)

	with db.cursor() as cur :
		cur.execute(
			loadQuery('query-unactivatedRequests.sql'),
			dict(

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
		result = unansweredRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'unansweredRequests-{}.csv'.format(inici))

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


	def _test_acceptedRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = acceptedRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'acceptedRequests-{}.csv'.format(inici))

	def test_acceptedRequests_2014_02(self) :
		self._test_acceptedRequests((2014,2))

	def test_acceptedRequests_2014_03(self) :
		self._test_acceptedRequests((2014,3))


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

	def _test_sentRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = sentRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'sentRequests-{}.csv'.format(inici))

	def test_sentRequests_2014_02(self) :
		self._test_sentRequests((2014,2))


	def _test_unactivatedRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = unactivatedRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'unactivatedRequests-{}.csv'.format(inici))

	def test_unactivatedRequests_2014_02(self) :
		self._test_unactivatedRequests((2014,2))








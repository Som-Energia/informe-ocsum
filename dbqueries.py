#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import psycopg2
import psycopg2.extras
import datetime
from dbutils import *
import dbconfig as config
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
				'A3', 'B1', 'C1', 'C2', 'D1', 'M1', 'R1', 'W1',
			])

	def test_idsPasses(self):
		passes = idsPasses(self.db)
		self.assertEqual(
			sorted(passes.keys()), [
				'A3_01', 'A3_02', 'A3_03', 'A3_04',
				'A3_05', 'A3_06', 'A3_07',
				'B1_01', 'B1_02', 'B1_03', 'B1_04', 'B1_05', 
				'C1_01', 'C1_02', 'C1_05', 'C1_06', 'C1_08',
				'C1_09', 'C1_10', 'C1_11',
				'C2_01', 'C2_02', 'C2_03', 'C2_04', 'C2_05',
				'C2_06', 'C2_07', 'C2_08', 'C2_09', 'C2_10',
				'C2_11', 'C2_12',
				'D1_01',
				'M1_01', 'M1_02', 'M1_03', 'M1_04', 'M1_05',
				'M1_06', 'M1_07', 'M1_08',
				'R1_01', 'R1_02', 'R1_03', 'R1_04', 'R1_05',
				'W1_01', 'W1_02',
			])

	def test_idsPasses_filteringByProcess(self):
		passes = idsPasses(self.db, 'B1', 'C2')
		self.assertEqual(
			sorted(passes.keys()), [
				'B1_01', 'B1_02', 'B1_03', 'B1_04', 'B1_05', 
				'C2_01', 'C2_02', 'C2_03', 'C2_04', 'C2_05',
				'C2_06', 'C2_07', 'C2_08', 'C2_09', 'C2_10',
				'C2_11', 'C2_12',
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
	# TODO: Group by 'TipoCambio' C4 si alta directa
	# TODO: Group by 'TipoPunto' depends on max power
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
		cur.execute(
			loadQuery('query-activatedRequests.sql'),
			dict(
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

def cancelledRequests(db, inici, final, cursorManager=nsList) :
	with db.cursor() as cur :
		cur.execute(
			loadQuery('query-cancelledRequests.sql'),
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

def dropoutRequests(db, inici, final, cursorManager=nsList):
	with db.cursor() as cur :
		cur.execute(
			loadQuery('query-dropoutRequests.sql'),
			dict(
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

	def test_rejectedRequests_2014_03(self) :
		self._test_rejectedRequests((2014,3))


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

	def test_activatedRequests_2014_03(self) :
		self._test_activatedRequests((2014,3))

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

	def _test_cancelledRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = cancelledRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'cancelledRequests-{}.csv'.format(inici))

	def test_cancelledRequests_2014_02(self) :
		self._test_cancelledRequests((2014,2))


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


	def _test_dropoutRequests(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = dropoutRequests(self.db, inici, final, cursorManager=csvTable)
		self.assertBack2Back(result, 'dropoutRequests-{}.csv'.format(inici))

	def test_dropoutRequests_2014_02(self) :
		self._test_dropoutRequests((2014,2))

	def test_dropoutRequests_2014_06(self) :
		self._test_dropoutRequests((2014,6))








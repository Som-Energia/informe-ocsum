#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import psycopg2
import psycopg2.extras
import datetime

from namespace import namespace as ns

def fetchNs(cursor):
	"""Wraps a database cursor so that instead of providing data
	as arrays, it provides objects with attributes named
	as the query column names."""
	fields = [column.name for column in cursor.description]
	for row in cursor:
		yield ns(zip(fields, row))
	raise StopIteration

def csvTable(cursor) :
	fields = [column.name for column in cursor.description]
	return '\n'.join('\t'.join(str(x) for x in line) for line in ([fields] + cursor.fetchall()) )

import unittest

class Back2BackTestCase(unittest.TestCase) :
	def assertBack2Back(self, result, testId) :
		def write(result) :
			with open(resultfilename,'w') as resultfile:
				resultfile.write(result)
		import os

		resultfilename = 'b2bdata/{}-result{}'.format(*os.path.splitext(testId))
		expectedfilename = 'b2bdata/{}-expected{}'.format(*os.path.splitext(testId))

		if os.access(resultfilename, os.F_OK) :
			os.unlink(resultfilename)

		try :
			with open(expectedfilename) as expectedfile:
				expected=expectedfile.read()
		except IOError as e:
			write(result)
			raise AssertionError("No expectation, accept with: mv {} {}".format(resultfilename, expectedfilename))

		self.maxDiff = None
		try:
			self.assertMultiLineEqual(expected, result)
		except AssertionError:
			import sys
	 		print("Back-to-back results differ, accept with: mv {} {}".format(resultfilename, expectedfilename))
			write(result)
			raise

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

class Test_switching(unittest.TestCase):

	def setUp(self):
		self.db = db

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



def peticionsPendentsDeResposta(db, inici, final):

	processos = idsProcessos(db)
	passes = idsPasses(db, "C1", 'C2')

	# TODO: S'esta fent servir incorrectament la creacio del cas com a data de carga del fitxer al sistema de la distribuidora
	# TODO: Group by 'TipoCambio' (siempre C3? cambio comercializadora. Cuando C4?)
	# TODO: Group by 'TipoPunto'
	# TODO: Group by 'Comer_saliente'
	# TODO: Group by 'Comer_entrante' (siempre 1 - Somenergia?)
	# TODO: Revisar interval de les dates
	# TODO: Plaç depenent de la tarifa
	# TODO: Tarifa s'agafa de l'actual, pot haver canviat

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
					sw.company_id AS company_id,
					dist.id AS distri,
					dist.ref AS refdistribuidora,
					dist.name AS nomdistribuidora,
					tar.name AS tarname,
					CASE
						WHEN tar.name = ANY( %(tarifesAltaTensio)s ) THEN
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

					/* No son de petites marcades com a aceptades sense 01 */
					case_.priority != '5' AND

					TRUE
				GROUP BY
					sw.id,
					sw.company_id,
					refdistribuidora,
					tarname,
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
		result = csvTable(cur)
		return result

class OcsumReport_Test(Back2BackTestCase) :

	def setUp(self):
		self.db = db

	def tearDown(self) :
		self.db.rollback()

	def _test_peticionsPendentsDeResposta(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = peticionsPendentsDeResposta(db, inici, final)
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

from dbconfig import psycopg as config

with psycopg2.connect(**config) as db:

	casosPerTipus = numeroDeCasos(db)
	print(casosPerTipus['C1']+casosPerTipus['C2'])

	import sys
	sys.exit(unittest.main())







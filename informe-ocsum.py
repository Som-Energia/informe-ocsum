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

	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				COUNT(*) AS nprocessos,
				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate, 
				codiprovincia,
				s.distri,
				s.tarname,
				s.refdistribuidora,
				nomprovincia,
				s.nomdistribuidora,
				STRING_AGG(s.sw_id::text, ',' ORDER BY s.sw_id),
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
				WHERE
					/* S'ha creat el cas abans del final del periode */
					sth.date_created < %(periodEnd)s AND 

					/* No s'ha fet l'alta abans de finalitzar el periode */
					(pol.data_alta IS NULL OR pol.data_alta>%(periodEnd)s ) AND

					/* Ens focalitzem en els processos indicats */
					sw.proces_id = ANY( %(process)s )  AND

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

class Back2Back(unittest.TestCase) :
	def assertBack2Back(self, result, testId) :
		def write(result) :
			with open(resultfilename,'w') as resultfile:
				resultfile.write(result)
		import os

		print (self.id().replace('__main__.','').replace(".", "/"))
		resultfilename = 'b2bdata/{}-result'.format(*os.path.splitext(testId))
		expectedfilename = 'b2bdata/{}-expected{}'.format(*os.path.splitext(testId))

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


with psycopg2.connect(dbname='somenergia') as db:

#	unittest.main()
	print("CACA")

	casosPerTipus = numeroDeCasos(db)
	print(casosPerTipus['C1']+casosPerTipus['C2'])

	import sys
	sys.exit(unittest.main())

	with db.cursor() as cur:
		cur.execute("""
			SELECT
				pr.name,
				pr.id
			FROM
				giscedata_switching_proces as pr
			""")
		processos = dict( (row.name, row.id) for row in fetchNs(cur))

	with db.cursor() as cur:
		cur.execute("""
			SELECT
				sw.id,
				sw.data_sollicitud,
				p2.id as p2_id,
				ph2.date_created as ph2_date_created,
				p2.data_acceptacio as p2_data_acceptacio, 
				p2.data_activacio as p2_data_activacio,
				p2.rebuig as p2_rebuig,
				p2.tipus_activacio as p2_tipus_activacio,
				p2.actuacio_camp as p2_actuacio_camp,
				current_date > sw.data_sollicitud+5 AS fuera,
				current_date > sw.data_sollicitud+15 AS muyFuera
			FROM
				giscedata_switching AS sw
			LEFT JOIN
				giscedata_switching_step_header AS ph1 ON ph1.sw_id = sw.id
			LEFT JOIN
				giscedata_switching_c1_01 AS p1 ON p1.header_id = ph1.id
			LEFT JOIN
				giscedata_switching_step_header AS ph2 ON ph2.sw_id = sw.id
			LEFT JOIN
				giscedata_switching_c1_02 AS p2 ON p2.header_id = ph2.id
			LEFT JOIN
				giscedata_switching_step_header AS ph5 ON ph5.sw_id = sw.id
			LEFT JOIN
				giscedata_switching_c1_05 AS p5 ON p5.header_id = ph5.id
			WHERE
				sw.data_sollicitud < %(finalPeriode)s AND
				sw.proces_id = %(process)s  AND
				p2.rebuig = %(rebuig)s

/*
			GROUP BY
				fuera,
				muyFuera
*/
			""",
			# Add literals here to avoid SQL injection
			dict(
				finalPeriode=datetime.date(2015,03,01),
				process = processos['C1'],
				rebuig=True,
			))

	# sw.data_sollicitud:
	# ph2.date_created: Considerem la data del rebuig en cas de C1_02
	# p2.rebuig

	with db.cursor() as cur:
		cur.execute("""
			SELECT 
				sw.id,
/*				count(sw.id),*/
				sw.data_sollicitud,
/*
				sw.*,
				p1.id as p1_id,
				p1.data_final as p1_data_final,
				p1.data_accio as p1_data_accio,
				p1.activacio_cicle as p1_activacio_cicle,
				p1.validacio_pendent as p1_validacio_pendent,
				p2.id as p2_id,
*/
				p2.id as p2_id,
				ph2.date_created as ph2_date_created,
				p2.data_acceptacio as p2_data_acceptacio, 
				p2.data_activacio as p2_data_activacio,
				p2.rebuig as p2_rebuig,
				p2.tipus_activacio as p2_tipus_activacio,
				p2.actuacio_camp as p2_actuacio_camp,
				current_date > sw.data_sollicitud+5 AS fuera,
				current_date > sw.data_sollicitud+15 AS muyFuera
			FROM
				giscedata_switching AS sw
/*
			LEFT JOIN
				giscedata_switching_step_header AS ph1 ON ph1.sw_id = sw.id
			LEFT JOIN
				giscedata_switching_c1_01 AS p1 ON p1.header_id = ph1.id
*/
			LEFT JOIN
				giscedata_switching_step_header AS ph2 ON ph2.sw_id = sw.id
			LEFT JOIN
				giscedata_switching_c1_02 AS p2 ON p2.header_id = ph2.id
/*			LEFT JOIN
				giscedata_switching_step_header AS ph5 ON ph5.sw_id = sw.id
			LEFT JOIN
				giscedata_switching_c1_05 AS p5 ON p5.header_id = ph5.id
*/
			WHERE
				sw.data_sollicitud >= %(inici)s AND
				sw.proces_id = %(process)s  AND
				p2.rebuig = %(rebuig)s

/*
			GROUP BY
				fuera,
				muyFuera
*/
			""",
			# Add literals here to avoid SQL injection
			dict(
				inici=datetime.date(2015,01,01),
				process = processos['C1'],
				rebuig=True,
			))

		print csvTable(cur)
#		print [row.p2_tipus_activacio for row in fetchNs(cur)]
#		print ns(data=[row.dump() for row in fetchNs(cur)]).dump()






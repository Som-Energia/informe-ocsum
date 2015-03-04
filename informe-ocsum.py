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
					sw.company_id AS company_id,
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
					sw.company_id,
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
		result = csvTable(cur)
		return result

def peticionsAcceptades(db, inici, final):

	# TODO:
	# - Date on 5-priority cases (accepted without a 02 step) migth not be real and outside the period.

	processos = idsProcessos(db)
	passes = idsPasses(db, "C1", 'C2')

	with db.cursor() as cur :
		cur.execute("""\
			SELECT
				COUNT(*) AS nprocessos,
				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN 1 ELSE 0 END) AS ontime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN 1 ELSE 0 END) AS late,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN 1 ELSE 0 END) AS verylate, 
/*				SUM(CASE WHEN (%(periodEnd)s > termini + interval '90 days') THEN 1 ELSE 0 END) AS unattended, */

				SUM(CASE WHEN (%(periodEnd)s <= termini) THEN
					DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END) AS ontimeAddedTime,
				SUM(CASE WHEN ((%(periodEnd)s > termini)  AND (%(periodEnd)s <= termini + interval '15 days')) THEN
					DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END) AS lateAddedTime,
				SUM(CASE WHEN (%(periodEnd)s > termini + interval '15 days') THEN
					DATE_PART('day', %(periodEnd)s - create_date) ELSE 0 END) AS verylateAddedTime, 
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
					sw.company_id AS company_id,
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
		result = csvTable(cur)
		return result

import b2btest

class OcsumReport_Test(b2btest.TestCase) :

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


	def _test_peticionsAcceptades(self, testcase) :
		year, month = testcase
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		result = peticionsAcceptades(db, inici, final)
		self.assertBack2Back(result, 'peticionsAcceptades-{}.csv'.format(inici))

	def test_peticionsAcceptades_2014_02(self) :
		self._test_peticionsAcceptades((2014,2))

	def test_peticionsAcceptades_2014_03(self) :
		self._test_peticionsAcceptades((2014,3))



from lxml import etree
class InformeSwitching:

	"""Codi tret de la taula mestra versió 4.1"""
	_codiTipusTarifa = dict(
		line.split()[::-1]
		for line in """\
			1T	2.1A  
			2	2.0DHA
			2S	2.0DHS
			2T	2.1DHA
			2V	2.1DHS
			3	3.0A  
			4	3.1A    
			5	6.1A  
			5T	6.1B  
			6	6.2  
			7	6.3    
			8	6.4    
			9	6.5    
			""".split('\n')
		if line.strip()
	)


	def __init__(self, **kw ) :
		self.__dict__.update(kw)
		self.canvis = {}

	def element(self, parent, name, content=None) :
		element = etree.Element(name)
		if parent is not None: parent.append(element)
		if content is not None: element.text = str(content)
		return element

	def generaPendents(self, parent, canvisPendents) :
		for codigoRetraso, n in [
				('00', canvisPendents.ontime),
				('05', canvisPendents.late),
				('15', canvisPendents.verylate),
				]:
			if not n: continue
			detalle = self.element(parent, 'DetallePendientesRespuesta')
			self.element(detalle, 'TipoRetraso', codigoRetraso)
			self.element(detalle, 'NumSolicitudesPendientes', n)

	def generaSolicituds(self, root):
		if not self.canvis : return
		solicitudes = self.element(root, 'SolicitudesRealizadas')
		for (
			provincia, distribuidora, tipoPunto, tipoTarifa
			),canvi  in self.canvis.iteritems():
				datos = self.element(solicitudes, 'DatosSolicitudes')
				self.element(datos, 'Provincia', provincia+'000')
				self.element(datos, 'Distribuidor', distribuidora)
				self.element(datos, 'Comer_entrante', 'R2-415')
				self.element(datos, 'Comer_saliente', '0')
				self.element(datos, 'TipoCambio', 'C3') # TODO
				self.element(datos, 'TipoPunto', tipoPunto) # TODO
				self.element(datos, 'TarifaATR', self._codiTipusTarifa[tipoTarifa]) # TODO

				self.element(datos, 'TotalSolicitudesEnviadas', 0) # TODO
				self.element(datos, 'SolicitudesAnuladas', 0) # TODO
				self.element(datos, 'Reposiciones', 0) # TODO: No ben definit
				self.element(datos, 'ClientesSalientes', 0) # TODO: 
				self.element(datos, 'NumImpagados', 0) # TODO: No ben definit

				self.generaPendents(datos, canvi.pendents)


	def genera(self) :
		etree.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
		root = self.element(None, 'MensajeSolicitudesRealizadas')
		xsdNs = '{http://www.w3.org/2001/XMLSchema-instance}'
		root.attrib[xsdNs+'noNamespaceSchemaLocation'] = 'SolicitudesRealizadas_v1.0.xsd'
		cabecera = self.element(root, 'Cabecera')
		self.element(cabecera, 'CodigoAgente', self.CodigoAgente)
		self.element(cabecera, 'TipoMercado', self.TipoMercado)
		self.element(cabecera, 'TipoAgente', self.TipoAgente)
		self.element(cabecera, 'Periodo', self.Periodo)

		self.generaSolicituds(root)

		return etree.tostring(
			root,
			pretty_print=True,
        	xml_declaration=True,
			encoding='utf-8',
        	method="xml",
			)

	def pendents(self,pendents) :
		pendent = pendents[0]
		key=(
			pendent.codiprovincia,
			pendent.refdistribuidora,
			1, # TODO
			pendent.tarname,
			)
		self.canvis.setdefault(key, ns()).pendents = pendent

class InformeSwitching_Test(unittest.TestCase) :
	def assertXmlEqual(self, got, want):
		from lxml.doctestcompare import LXMLOutputChecker
		from doctest import Example

		checker = LXMLOutputChecker()
		if checker.check_output(want, got, 0):
			return
		message = checker.output_difference(Example("", want), got, 0)
		raise AssertionError(message)

	head = """\
<?xml version="1.0" encoding="UTF-8"?>
<MensajeSolicitudesRealizadas
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
	xsi:noNamespaceSchemaLocation="SolicitudesRealizadas_v1.0.xsd"
>
	<Cabecera>
		<CodigoAgente>R2-415</CodigoAgente>
		<TipoMercado>E</TipoMercado>
		<TipoAgente>C</TipoAgente>
		<Periodo>201501</Periodo>
	</Cabecera>
"""
	foot = """\
</MensajeSolicitudesRealizadas>
"""

	def test_genera_senseSolicituds(self) :
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		self.assertXmlEqual(
			informe.genera(),
			self.head+self.foot
			)
	def test_genera_solicitudsPendents(self) :
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.pendents( [
			ns(
				nprocessos=300,
				ontime=300,
				late=0,
				verylate=0, 
				codiprovincia='08',
				tarname='2.0DHA',
				refdistribuidora='R1-001',
				),
			])
		
		self.assertXmlEqual(
			informe.genera(),
			self.head +
			"""\
	<SolicitudesRealizadas>
		<DatosSolicitudes>
			<Provincia>08000</Provincia>
			<Distribuidor>R1-001</Distribuidor>
			<Comer_entrante>R2-415</Comer_entrante>
			<Comer_saliente>0</Comer_saliente>
			<TipoCambio>C3</TipoCambio>
			<TipoPunto>1</TipoPunto>
			<TarifaATR>2</TarifaATR>
			<TotalSolicitudesEnviadas>0</TotalSolicitudesEnviadas>
			<SolicitudesAnuladas>0</SolicitudesAnuladas>
			<Reposiciones>0</Reposiciones>
			<ClientesSalientes>0</ClientesSalientes>
			<NumImpagados>0</NumImpagados>
			<DetallePendientesRespuesta>
				<TipoRetraso>00</TipoRetraso>
				<NumSolicitudesPendientes>300</NumSolicitudesPendientes>
			</DetallePendientesRespuesta>
		</DatosSolicitudes>
	</SolicitudesRealizadas>
""" + self.foot
			)

	def test_genera_solicitudsPendents_retrasades(self) :
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.pendents( [
			ns(
				nprocessos=600,
				ontime=100,
				late=200,
				verylate=300, 
				codiprovincia='08',
				tarname='2.0DHA',
				refdistribuidora='R1-001',
				),
			])
		
		self.assertXmlEqual(
			informe.genera(),
			self.head +
			"""\
	<SolicitudesRealizadas>
		<DatosSolicitudes>
			<Provincia>08000</Provincia>
			<Distribuidor>R1-001</Distribuidor>
			<Comer_entrante>R2-415</Comer_entrante>
			<Comer_saliente>0</Comer_saliente>
			<TipoCambio>C3</TipoCambio>
			<TipoPunto>1</TipoPunto>
			<TarifaATR>2</TarifaATR>
			<TotalSolicitudesEnviadas>0</TotalSolicitudesEnviadas>
			<SolicitudesAnuladas>0</SolicitudesAnuladas>
			<Reposiciones>0</Reposiciones>
			<ClientesSalientes>0</ClientesSalientes>
			<NumImpagados>0</NumImpagados>
			<DetallePendientesRespuesta>
				<TipoRetraso>00</TipoRetraso>
				<NumSolicitudesPendientes>100</NumSolicitudesPendientes>
			</DetallePendientesRespuesta>
			<DetallePendientesRespuesta>
				<TipoRetraso>05</TipoRetraso>
				<NumSolicitudesPendientes>200</NumSolicitudesPendientes>
			</DetallePendientesRespuesta>
			<DetallePendientesRespuesta>
				<TipoRetraso>15</TipoRetraso>
				<NumSolicitudesPendientes>300</NumSolicitudesPendientes>
			</DetallePendientesRespuesta>
		</DatosSolicitudes>
	</SolicitudesRealizadas>
""" + self.foot
			)


from dbconfig import psycopg as config

with psycopg2.connect(**config) as db:
	import sys
	sys.exit(unittest.main())







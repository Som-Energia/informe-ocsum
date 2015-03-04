#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import psycopg2
import psycopg2.extras
import datetime

from namespace import namespace as ns
import b2btest

from dbqueries import *
from decimal import Decimal

from lxml import etree
class InformeSwitching:

	def __init__(self, **kw ) :

		self.__dict__.update(kw)
		self.canvis = {}

		# Codi tret de la taula mestra versió 4.1
		self._codiTipusTarifa = dict(
			line.split()[::-1]
			for line in """\
				1	2.0A  
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


	def element(self, parent, name, content=None) :
		element = etree.Element(name)
		if parent is not None: parent.append(element)
		if content is not None: element.text = str(content)
		return element

	def genera(self) :
		xsiNs = 'http://www.w3.org/2001/XMLSchema-instance'
		xsi = '{'+xsiNs +'}'
		schema = 'SolicitudesRealizadas_v1.0.xsd'
		etree.register_namespace('xsi', xsiNs)
		root = self.element(None, 'MensajeSolicitudesRealizadas')
		root.attrib[xsi+'noNamespaceSchemaLocation'] = schema
		self.generateHeader(root)
		self.generateRequestSummaries(root)

		return etree.tostring(
			root,
			pretty_print=True,
        	xml_declaration=True,
			encoding='utf-8',
        	method="xml",
			)

	def generateHeader(self, parent):
		cabecera = self.element(parent, 'Cabecera')
		self.element(cabecera, 'CodigoAgente', self.CodigoAgente)
		self.element(cabecera, 'TipoMercado', self.TipoMercado)
		self.element(cabecera, 'TipoAgente', self.TipoAgente)
		self.element(cabecera, 'Periodo', self.Periodo)

	def generateRequestSummaries(self, root):
		if not self.canvis : return
		solicitudes = self.element(root, 'SolicitudesRealizadas')
		for (
			provincia, distribuidora, tipoPunto, tipoTarifa
			),canvi  in sorted(self.canvis.iteritems()):
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

				if 'pendents' in canvi :
					self.generatePendingDetails(datos, canvi.pendents)

				if 'accepted' in canvi :
					self.generateAcceptedDetails(datos, canvi.accepted)

	def generatePendingDetails(self, parent, canvisPendents) :
		for codigoRetraso, n in [
				('00', canvisPendents.ontime),
				('05', canvisPendents.late),
				('15', canvisPendents.verylate),
				]:
			if not n: continue
			detail = self.element(parent, 'DetallePendientesRespuesta')
			self.element(detail, 'TipoRetraso', codigoRetraso)
			self.element(detail, 'NumSolicitudesPendientes', n)

	def generateAcceptedDetails(self, parent, accepted):
		for codigoRetraso, n, addedTime in [
				('00', accepted.ontime, accepted.ontimeaddedtime),
				('05', accepted.late, accepted.lateaddedtime),
				('15', accepted.verylate, accepted.verylateaddedtime),
			]:

			if not n : continue
			meanTime = Decimal(addedTime) / n
			detail = self.element(parent, 'DetalleAceptadas')
			self.element(detail, 'TipoRetraso', codigoRetraso)
			self.element(detail, 'TMSolicitudesAceptadas', '{:.1f}'.format(meanTime))
			self.element(detail, 'NumSolicitudesAceptadas', n)

	def fillPending(self,pendents) :
		for pendent in pendents:
			key=(
				pendent.codiprovincia,
				pendent.refdistribuidora,
				1, # TODO
				pendent.tarname,
				)
			self.canvis.setdefault(key, ns()).pendents = pendent

	def fillAccepted(self, acceptedSummary) :
		for accepted in acceptedSummary:
			key=(
				accepted.codiprovincia,
				accepted.refdistribuidora,
				1, # TODO
				accepted.tarname,
				)
			self.canvis.setdefault(key, ns()).accepted = accepted


class InformeSwitching_Test(unittest.TestCase) :

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
		informe.fillPending( [
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
		informe.fillPending( [
			ns(
				nprocessos=600,
				ontime=100,
				late=200,
				verylate=300, 
				codiprovincia='08',
				tarname='2.0DHA',
				refdistribuidora='R1-002',
				),
			])

		self.assertXmlEqual(
			informe.genera(),
			self.head +
			"""\
	<SolicitudesRealizadas>
		<DatosSolicitudes>
			<Provincia>08000</Provincia>
			<Distribuidor>R1-002</Distribuidor>
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
	def test_genera_solicitudsPendents_diversesComercialitzadores(self) :
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillPending( [
			ns(
				nprocessos=300,
				ontime=300,
				late=0,
				verylate=0, 
				codiprovincia='08',
				tarname='2.0DHA',
				refdistribuidora='R1-001',
				),
			ns(
				nprocessos=600,
				ontime=100,
				late=200,
				verylate=300, 
				codiprovincia='08',
				tarname='2.0DHA',
				refdistribuidora='R1-002',
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
		<DatosSolicitudes>
			<Provincia>08000</Provincia>
			<Distribuidor>R1-002</Distribuidor>
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
	def test_genera_solicitudsAcceptades(self) :
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillAccepted( [
			ns(
				nprocessos=300,
				ontime=300,
				late=0,
				verylate=0, 
				ontimeaddedtime=450,
				lateaddedtime=0,
				verylateaddedtime=0,
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
			<DetalleAceptadas>
				<TipoRetraso>00</TipoRetraso>
				<TMSolicitudesAceptadas>1.5</TMSolicitudesAceptadas>
				<NumSolicitudesAceptadas>300</NumSolicitudesAceptadas>
			</DetalleAceptadas>
		</DatosSolicitudes>
	</SolicitudesRealizadas>
""" + self.foot
			)

	def test_genera_solicitudsAcceptades_delayed(self) :
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillAccepted( [
			ns(
				nprocessos=300,
				ontime=0,
				late=200,
				verylate=100, 
				ontimeaddedtime=0,
				lateaddedtime=3200,
				verylateaddedtime=2000,
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
			<DetalleAceptadas>
				<TipoRetraso>05</TipoRetraso>
				<TMSolicitudesAceptadas>16.0</TMSolicitudesAceptadas>
				<NumSolicitudesAceptadas>200</NumSolicitudesAceptadas>
			</DetalleAceptadas>
			<DetalleAceptadas>
				<TipoRetraso>15</TipoRetraso>
				<TMSolicitudesAceptadas>20.0</TMSolicitudesAceptadas>
				<NumSolicitudesAceptadas>100</NumSolicitudesAceptadas>
			</DetalleAceptadas>
		</DatosSolicitudes>
	</SolicitudesRealizadas>
""" + self.foot
			)


class XmlGenerateFromDb_Test(b2btest.TestCase) :

	def test_fullGenerate(self):
		"""Work In progress as we get it assembled"""

		year, month = (2014,2)
		inici=datetime.date(year,month,1)
		try:
			final=datetime.date(year,month+1,1)
		except ValueError:
			final=datetime.date(year+1,1,1)
		informe = InformeSwitching(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='{}{:02}'.format(year, month),
			)
		from dbconfig import psycopg as config

		with psycopg2.connect(**config) as db:
			pendents=peticionsPendentsDeResposta(db, inici, final)
			acceptades=peticionsAcceptades(db, inici, final)

		informe.fillPending( pendents )
		informe.fillAccepted( acceptades )


		result = informe.genera()

		self.assertBack2Back(result, 'informeOcsum-{}.xml'.format(inici))


if __name__ == '__main__' :
	import sys
	sys.exit(unittest.main())







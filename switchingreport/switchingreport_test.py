#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import datetime
from yamlns import namespace as ns
from dbqueries import *
#from debugcase import *
from decimal import Decimal
from lxml import etree
from consolemsg import step, warn
try: import dbconfig as config
except ImportError: config=None
from .switchingreport import *


import unittest

class SwichingReport_Test(unittest.TestCase) :

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
	summaryHead = """\
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
"""
	summaryHead2 = """\
	<SolicitudesRealizadas>
		<DatosSolicitudes>
			<Provincia>08000</Provincia>
			<Distribuidor>R1-001</Distribuidor>
			<Comer_entrante>R2-415</Comer_entrante>
			<Comer_saliente>0</Comer_saliente>
			<TipoCambio>C3</TipoCambio>
			<TipoPunto>1</TipoPunto>
			<TarifaATR>2</TarifaATR>
			<TotalSolicitudesEnviadas>2</TotalSolicitudesEnviadas>
			<SolicitudesAnuladas>0</SolicitudesAnuladas>
			<Reposiciones>0</Reposiciones>
			<ClientesSalientes>0</ClientesSalientes>
			<NumImpagados>0</NumImpagados>
"""
	secondSummaryHeader = """\
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
"""
	summaryFoot = """\
		</DatosSolicitudes>
	</SolicitudesRealizadas>
"""

	def test_genera_senseSolicituds(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		self.assertXmlEqual(
			informe.genera(),
			self.head+self.foot
			)

	def test_genera_nonDetail(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillSent([
			ns(
				nreq=2,
				nprocessos=300,
				ontime=300,
				late=0,
				verylate=0, 
				codiprovincia='08',
				tarname='2.0DHA',
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])
		self.assertXmlEqual(
			informe.genera(),
			self.head+self.summaryHead2+self.summaryFoot+self.foot
			)

	def test_genera_solicitudsPendents(self) :
		informe = SwichingReport(
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
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])

		self.assertXmlEqual(
			informe.genera(),
			self.head +
			self.summaryHead +
			"""\
			<DetallePendientesRespuesta>
				<TipoRetraso>00</TipoRetraso>
				<NumSolicitudesPendientes>300</NumSolicitudesPendientes>
			</DetallePendientesRespuesta>
""" + self.summaryFoot + self.foot
			)

	def test_genera_solicitudsPendents_retrasades(self) :
		informe = SwichingReport(
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
				tipocambio='C3',
				tipopunto='1',
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
		informe = SwichingReport(
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
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			ns(
				nprocessos=600,
				ontime=100,
				late=200,
				verylate=300, 
				codiprovincia='08',
				tarname='2.0DHA',
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-002',
				),
			])

		self.assertXmlEqual(
			informe.genera(),
			self.head +
			self.summaryHead +
			"""\
			<DetallePendientesRespuesta>
				<TipoRetraso>00</TipoRetraso>
				<NumSolicitudesPendientes>300</NumSolicitudesPendientes>
			</DetallePendientesRespuesta>
""" + self.secondSummaryHeader + """\
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
""" + self.summaryFoot + self.foot
			)
	def test_genera_solicitudsAcceptades(self) :
		informe = SwichingReport(
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
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])

		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
			"""\
			<DetalleAceptadas>
				<TipoRetraso>00</TipoRetraso>
				<TMSolicitudesAceptadas>1.5</TMSolicitudesAceptadas>
				<NumSolicitudesAceptadas>300</NumSolicitudesAceptadas>
			</DetalleAceptadas>
"""
			+ self.summaryFoot
			+ self.foot
			)

	def test_genera_solicitudsAcceptades_delayed(self) :
		informe = SwichingReport(
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
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])

		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
			"""\
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
""" + self.summaryFoot +  self.foot
			)


	def test_genera_rejectedRequest_single(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillRejected( [
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
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				rejectreason='03',
				),
			])
		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
			"""\
			<DetalleRechazadas>
				<TipoRetraso>00</TipoRetraso>
				<TMSolicitudesRechazadas>1.5</TMSolicitudesRechazadas>
				<MotivoRechazo>03</MotivoRechazo>
				<NumSolicitudesRechazadas>300</NumSolicitudesRechazadas>
			</DetalleRechazadas>
""" + self.summaryFoot + self.foot
			)
	def test_genera_rejectedRequest_multipleDistros(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillRejected( [
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
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				rejectreason='03',
				),
			ns(
				nprocessos=200,
				ontime=200,
				late=0,
				verylate=0, 
				ontimeaddedtime=1000,
				lateaddedtime=0,
				verylateaddedtime=0,
				codiprovincia='08',
				tarname='2.0DHA',
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-002',
				rejectreason='01',
				),
			])
		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
			"""\
			<DetalleRechazadas>
				<TipoRetraso>00</TipoRetraso>
				<TMSolicitudesRechazadas>1.5</TMSolicitudesRechazadas>
				<MotivoRechazo>03</MotivoRechazo>
				<NumSolicitudesRechazadas>300</NumSolicitudesRechazadas>
			</DetalleRechazadas>
""" + self.secondSummaryHeader + """\
			<DetalleRechazadas>
				<TipoRetraso>00</TipoRetraso>
				<TMSolicitudesRechazadas>5.0</TMSolicitudesRechazadas>
				<MotivoRechazo>01</MotivoRechazo>
				<NumSolicitudesRechazadas>200</NumSolicitudesRechazadas>
			</DetalleRechazadas>
""" + self.summaryFoot + self.foot
			)

	def test_genera_activationPendingRequest_ontime(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillActivationPending( [
			ns(
				nprocessos=300,
				ontime=300,
				late=0,
				verylate=0, 
				ontimeissues=0,
				lateissues=0,
				verylateissues=0, 
				codiprovincia='08',
				tarname='2.0DHA',
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])
		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
		"""\
			<DetallePdteActivacion>
				<TipoRetraso>00</TipoRetraso>
				<NumIncidencias>0</NumIncidencias>
				<NumSolicitudesPdteActivacion>300</NumSolicitudesPdteActivacion>
			</DetallePdteActivacion>
""" + self.summaryFoot + self.foot
			)

	def test_genera_activationPendingRequest_delayed(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillActivationPending( [
			ns(
				nprocessos=300,
				ontime=0,
				late=200,
				verylate=100, 
				ontimeissues=0,
				lateissues=0,
				verylateissues=0, 
				codiprovincia='08',
				tarname='2.0DHA',
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])
		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
		"""\
			<DetallePdteActivacion>
				<TipoRetraso>05</TipoRetraso>
				<NumIncidencias>0</NumIncidencias>
				<NumSolicitudesPdteActivacion>200</NumSolicitudesPdteActivacion>
			</DetallePdteActivacion>
			<DetallePdteActivacion>
				<TipoRetraso>15</TipoRetraso>
				<NumIncidencias>0</NumIncidencias>
				<NumSolicitudesPdteActivacion>100</NumSolicitudesPdteActivacion>
			</DetallePdteActivacion>
""" + self.summaryFoot + self.foot
			)
	def test_genera_activatedRequest_ontime(self) :
		informe = SwichingReport(
			CodigoAgente='R2-415',
			TipoMercado='E',
			TipoAgente='C',
			Periodo='201501',
			)
		informe.fillActivated( [
			ns(
				nprocessos=300,
				ontime=300,
				late=0,
				verylate=0, 
				ontimeaddedtime=6000,
				lateaddedtime=0,
				verylateaddedtime=0,
				ontimeissues=0,
				lateissues=0,
				verylateissues=0, 
				codiprovincia='08',
				tarname='2.0DHA',
				tipocambio='C3',
				tipopunto='1',
				refdistribuidora='R1-001',
				),
			])
		self.assertXmlEqual(
			informe.genera(),
			self.head + self.summaryHead +
		"""\
			<DetalleActivadas>
				<TipoRetraso>00</TipoRetraso>
				<TMActivacion>20.0</TMActivacion>
				<NumIncidencias>0</NumIncidencias>
				<NumSolicitudesActivadas>300</NumSolicitudesActivadas>
			</DetalleActivadas>
""" + self.summaryFoot + self.foot
			)

import b2btest


@unittest.skipIf(config is None, "No dbconfig.py found")
class XmlGenerateFromDb_Test(b2btest.TestCase) :

	def test_fullGenerate(self):

		year, month = (2014,2)
		agent = 'R2-415'
		inici=datetime.date(year,month,1)
		result = fullGenerate(year,month, agent)
		self.assertBack2Back(result, 'informeOcsum-{}.xml'.format(inici))


unittest.TestCase.__str__ = unittest.TestCase.id





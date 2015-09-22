#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import datetime
from namespace import namespace as ns
from dbqueries import *
from debugcase import *
from decimal import Decimal
from lxml import etree
from consolemsg import step, warn

class SwichingReport:

	def __init__(self, **kw ) :

		self.__dict__.update(kw)
		self.canvis = {}

		# Codi tret de la taula mestra versió 4.1
		self._fareCodes = dict(
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
		for keys, canvi in sorted(self.canvis.iteritems()):
			provincia, distribuidora, tipoCambio, tipoPunto, tipoTarifa = keys
			if any(key is None for key in keys):
				warn("A key has a None value ({}) : {}".format(
					keys, canvi.dump()))
				continue
			datos = self.element(solicitudes, 'DatosSolicitudes')
			self.element(datos, 'Provincia', provincia+'000')
			self.element(datos, 'Distribuidor', distribuidora)
			self.element(datos, 'Comer_entrante', 'R2-415')
			self.element(datos, 'Comer_saliente', '0')
			self.element(datos, 'TipoCambio', tipoCambio) # TODO
			self.element(datos, 'TipoPunto', tipoPunto) # TODO
			self.element(datos, 'TarifaATR', self._fareCodes[tipoTarifa]) # TODO

			self.element(datos, 'TotalSolicitudesEnviadas', canvi.get('sent',0))
			self.element(datos, 'SolicitudesAnuladas', canvi.get('cancelled',0))
			self.element(datos, 'Reposiciones', 0) # TODO: No ben definit
			self.element(datos, 'ClientesSalientes', canvi.get('dropouts',0))
			self.element(datos, 'NumImpagados', 0) # TODO: No ben definit

			if 'pendents' in canvi :
				self.generatePendingDetails(datos, canvi.pendents)

			if 'accepted' in canvi :
				self.generateAcceptedDetails(datos, canvi.accepted)

			if 'rejected' in canvi :
				for rejected in canvi.rejected :
					self.generateRejectedDetails(datos, rejected)

			if 'activationPending' in canvi :
				self.generateActivationPendingDetails(datos, canvi.activationPending)

			if 'activated' in canvi :
				self.generateActivated(datos, canvi.activated)

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

	def generateAcceptedDetails(self, parent, summary):
		for codigoRetraso, n, addedTime in [
				('00', summary.ontime, summary.ontimeaddedtime),
				('05', summary.late, summary.lateaddedtime),
				('15', summary.verylate, summary.verylateaddedtime),
				]:

			if not n : continue
			meanTime = Decimal(addedTime) / n
			detail = self.element(parent, 'DetalleAceptadas')
			self.element(detail, 'TipoRetraso', codigoRetraso)
			self.element(detail, 'TMSolicitudesAceptadas', '{:.1f}'.format(meanTime))
			self.element(detail, 'NumSolicitudesAceptadas', n)

	def generateRejectedDetails(self, parent, rejected):
		for codigoRetraso, n, addedTime in [
				('00', rejected.ontime, rejected.ontimeaddedtime),
				('05', rejected.late, rejected.lateaddedtime),
				('15', rejected.verylate, rejected.verylateaddedtime),
				]:

			if not n : continue
			meanTime = Decimal(addedTime) / n
			detail = self.element(parent, 'DetalleRechazadas')
			self.element(detail, 'TipoRetraso', codigoRetraso)
			self.element(detail, 'TMSolicitudesRechazadas', '{:.1f}'.format(meanTime))
			self.element(detail, 'MotivoRechazo', rejected.rejectreason)
			self.element(detail, 'NumSolicitudesRechazadas', n)

	def generateActivationPendingDetails(self, parent, summary) :
		for codigoRetraso, n, issues in [
				('00', summary.ontime, summary.ontimeissues),
				('05', summary.late, summary.lateissues),
				('15', summary.verylate, summary.verylateissues),
				]:

			if not n : continue
			detail = self.element(parent, 'DetallePdteActivacion')
			self.element(detail, 'TipoRetraso', codigoRetraso)
			self.element(detail, 'NumIncidencias', issues)
			self.element(detail, 'NumSolicitudesPdteActivacion', n)

	def generateActivated(self, parent, summary) :
		for codigoRetraso, n, addedTime, issues in [
				('00', summary.ontime, summary.ontimeaddedtime, summary.ontimeissues),
				('05', summary.late, summary.lateaddedtime, summary.lateissues),
				('15', summary.verylate, summary.verylateaddedtime, summary.verylateissues),
				]:

			if not n : continue
			meanTime = Decimal(addedTime) / n
			detail = self.element(parent, 'DetalleActivadas')
			self.element(detail, 'TipoRetraso', codigoRetraso)
			self.element(detail, 'TMActivacion', '{:.1f}'.format(meanTime))
			self.element(detail, 'NumIncidencias', issues)
			self.element(detail, 'NumSolicitudesActivadas', n)

	def details(self, key) :
		return self.canvis.setdefault(key, ns())

	def fillSent(self,sents) :
		for sent in sents:
			key=(
				sent.codiprovincia,
				sent.refdistribuidora,
				sent.tipocambio,
				1, # TODO
				sent.tarname,
				)
			self.details(key).sent = sent.nreq

	def fillDropOuts(self, dropouts):
		for dropout in dropouts:
			key=(
				dropout.codiprovincia,
				dropout.refdistribuidora,
				dropout.tipocambio,
				1, # TODO
				dropout.tarname,
				)
			self.details(key).dropouts = dropout.nreq

	def fillCancelled(self,cancelledOnes) :
		for cancelled in cancelledOnes:
			key=(
				cancelled.codiprovincia,
				cancelled.refdistribuidora,
				cancelled.tipocambio,
				1, # TODO
				cancelled.tarname,
				)
			self.details(key).cancelled = cancelled.nreq

	def fillPending(self,pendents) :
		for pendent in pendents:
			key=(
				pendent.codiprovincia,
				pendent.refdistribuidora,
				'C3', # tipoCambio
				1, # TODO
				pendent.tarname,
				)
			self.details(key).pendents = pendent

	def fillAccepted(self, sumaries) :
		for summary in sumaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				1, # TODO
				summary.tarname,
				)
			self.details(key).accepted = summary

	def fillRejected(self, summaries):
		for summary in summaries:
			key = (
				summary.codiprovincia,
				summary.refdistribuidora,
				'C3', # tipoCambio
				1, # TODO
				summary.tarname,
				)
			# More than one entry (for each different reason
			self.details(key).setdefault('rejected',[]).append(summary)

	def fillActivationPending(self, summaries) :
			summary = summaries[0]
			key = (
				summary.codiprovincia,
				summary.refdistribuidora,
				'C3', # tipoCambio
				1, # TODO
				summary.tarname,
				)
			self.details(key).activationPending = summary

	def fillActivated(self, summaries) :
		for summary in summaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				1, # TODO
				summary.tarname,
				)
			self.details(key).activated = summary


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

def fullGenerate(year, month, agent):
	inici=datetime.date(year,month,1)
	try:
		final=datetime.date(year,month+1,1)
	except ValueError:
		final=datetime.date(year+1,1,1)
	informe = SwichingReport(
		CodigoAgente=agent,
		TipoMercado='E',
		TipoAgente='C',
		Periodo='{}{:02}'.format(year, month),
		)
	from dbconfig import psycopg as config

	import psycopg2
	with psycopg2.connect(**config) as db:
		pendents=unansweredRequests(db, inici, final)
		acceptades=acceptedRequests(db, inici, final)
		rejected=rejectedRequests(db, inici, final)
		activated=activatedRequests(db, inici, final)
		sent=sentRequests(db, inici, final)
		cancelled=cancelledRequests(db, inici, final)
		dropouts=dropoutRequests(db, inici, final)

	informe.fillPending( pendents )
	informe.fillAccepted( acceptades )
	informe.fillRejected( rejected )
	informe.fillActivated( activated )
	informe.fillSent( sent )
	informe.fillCancelled( cancelled )
	informe.fillDropOuts( dropouts )

	result = informe.genera()
	return result

def reportName(year, month, agent, sequence=1):
	return 'SI_{}_{}_{:04}{:02}_{}.xml'.format(
		agent, 'E',
		year,
		month,
		sequence,
		)


@unittest.skipIf(config is None, "No dbconfig.py found")
class XmlGenerateFromDb_Test(b2btest.TestCase) :

	def test_fullGenerate(self):
		"""Work In progress as we get it assembled"""

		year, month = (2014,2)
		agent = 'R2-415'
		inici=datetime.date(year,month,1)
		result = fullGenerate(year,month, agent)
		self.assertBack2Back(result, 'informeOcsum-{}.xml'.format(inici))


if __name__ == '__main__' :
	import sys
	if '--test' in sys.argv:
		sys.argv.remove('--test')
		sys.exit(unittest.main())

	import argparse
	parser = argparse.ArgumentParser(description='Generates switching monthly report')
	parser.add_argument('year', type=int, metavar='YEAR')
	parser.add_argument('month', type=int, metavar='MONTH')
	parser.add_argument('sequence', type=int, metavar='SEQUENCE', nargs='?')
	parser.add_argument('--agent', metavar='AGENT', nargs='?', default='R2-415', help='Agent code ')
	parser.add_argument('--csv', help='Agent code ', action='store_true')
	args = parser.parse_args()
	sequence = args.sequence

	inici=datetime.date(args.year,args.month,1)
	try:
		final=datetime.date(args.year,args.month+1,1)
	except ValueError:
		final=datetime.date(args.year+1,1,1)

	if args.csv:
		from dbconfig import psycopg as config

		import psycopg2
		with psycopg2.connect(**config) as db:
			for f in (
				unansweredRequests,
				acceptedRequests,
				rejectedRequests,
				activatedRequests,
				sentRequests,
				cancelledRequests,
				dropoutRequests,
				) :
				results = f(db, inici, final, cursorManager=csvTable)
				csvname = 'report-{:04}{:02}-{}.csv'.format(
					args.year, args.month, f.__name__)
				step("Generating '{}'...".format(csvname))
				with open(csvname,'w') as output:
					output.write(results)
		sys.exit(0)

	if args.sequence is None:
		args.sequence = 0
		while True:
			args.sequence+=1
			xmlname = reportName(args.year, args.month, args.agent, args.sequence)
			if os.access(xmlname, os.F_OK) : continue
			break
	else:
		xmlname = reportName(args.year, args.month, args.agent, args.sequence)
		
	step("Collecting data...")
	result = fullGenerate(args.year, args.month, args.agent)
	step("Writing {}...".format(xmlname))
	with open(xmlname, 'w') as xml:
		xml.write(result)

unittest.TestCase.__str__ = unittest.TestCase.id





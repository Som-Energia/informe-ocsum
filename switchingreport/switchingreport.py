#!/usr/bin/env python
#-*- encoding: utf-8 -*-

from yamlns import namespace as ns
from dbqueries import *
from dbutils import csvTable
from decimal import Decimal
from lxml import etree
from consolemsg import step, warn

try: import dbconfig as config
except ImportError: config=None


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
				warn("A key has a None value ({}):\n{}".format(
					keys, canvi.dump()))
				continue
			datos = self.element(solicitudes, 'DatosSolicitudes')
			self.element(datos, 'Provincia', provincia+'000')
			self.element(datos, 'Distribuidor', distribuidora)
			self.element(datos, 'Comer_entrante', self.CodigoAgente)
			self.element(datos, 'Comer_saliente', '0')
			self.element(datos, 'TipoCambio', tipoCambio)
			self.element(datos, 'TipoPunto', tipoPunto) # TODO
			self.element(datos, 'TarifaATR', self._fareCodes[tipoTarifa])

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

	def fillSent(self,summaries) :
		for summary in summaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).sent = summary.nreq

	def fillDropOuts(self, summaries):
		for summary in summaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).dropouts = summary.nreq

	def fillCancelled(self,summaries) :
		for summary in summaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).cancelled = summary.nreq

	def fillPending(self,summaries) :
		for summary in summaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).pendents = summary

	def fillAccepted(self, sumaries) :
		for summary in sumaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).accepted = summary

	def fillRejected(self, summaries):
		for summary in summaries:
			key = (
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			# More than one entry (for each different reason
			self.details(key).setdefault('rejected',[]).append(summary)

	def fillActivationPending(self, summaries) :
			summary = summaries[0]
			key = (
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).activationPending = summary

	def fillActivated(self, summaries) :
		for summary in summaries:
			key=(
				summary.codiprovincia,
				summary.refdistribuidora,
				summary.tipocambio,
				summary.tipopunto,
				summary.tarname,
				)
			self.details(key).activated = summary

def monthBounds(year, month):
	import datetime
	inici=datetime.date(year,month,1)
	try:
		final=datetime.date(year,month+1,1)
	except ValueError:
		final=datetime.date(year+1,1,1)
	return inici, final


def generateCsv(year, month):
	inici,final = monthBounds(year, month)

	from dbconfig import psycopg as config

	import psycopg2
	with psycopg2.connect(**config) as db:
		for f in (
				unansweredRequests,
				unactivatedRequests,
				acceptedRequests,
				rejectedRequests,
				activatedRequests,
				sentRequests,
				cancelledRequests,
				dropoutRequests,
				) :
			results = f(db, inici, final, cursorManager=csvTable)
			csvname = 'report-{:04}{:02}-{}.csv'.format(
				year, month, f.__name__)
			step("Generating '{}'...".format(csvname))
			with open(csvname,'w') as output:
				output.write(results)

def fullGenerate(year, month, agent):
	inici,final = monthBounds(year, month)

	informe = SwichingReport(
		CodigoAgente=agent,
		TipoMercado='E',
		TipoAgente='C',
		Periodo='{}{:02}'.format(year, month),
		)
	from dbconfig import psycopg as config

	import psycopg2
	with psycopg2.connect(**config) as db:
		for request, filler in (
			(unansweredRequests, informe.fillPending),
			(unactivatedRequests, informe.fillActivationPending),
			(acceptedRequests, informe.fillAccepted),
			(rejectedRequests, informe.fillRejected),
			(activatedRequests, informe.fillActivated),
			(sentRequests, informe.fillSent),
			(cancelledRequests, informe.fillCancelled),
			(dropoutRequests, informe.fillDropOuts),
			):
			data=request(db, inici, final)
			filler(data)

	result = informe.genera()
	return result

def reportName(year, month, agent, sequence=1):
	return 'SI_{}_{}_{:04}{:02}_{:02}.xml'.format(
		agent, 'E',
		year,
		month,
		sequence,
		)





# vim: ts=4 sw=4 noet

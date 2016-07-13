#!/usr/bin/env python
#-*- encoding: utf-8 -*-

from dbutils import nsList, fetchNs

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







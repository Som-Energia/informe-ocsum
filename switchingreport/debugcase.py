#!/usr/bin/env python
#-*- encoding: utf-8 -*-

from dbutils import *
from dbqueries import loadQuery, idsProcessos, idsPasses
import psycopg2
import sys
import os
from yamlns import namespace as ns
from consolemsg import warn, fail
import dbconfig as config

def anonyfy(text):
	return ' '.join( w[0]+'.'*len(w[:-2])+w[-1] for  w in text.split())

def impersonatePersonalData(detail):
	personalKeys = set(
		"nom cognom_1 cognom_2 codi_document telefon observacions ref_dist name".split())
	for key in personalKeys:
		if key in detail:
			detail[key] = detail[key] and anonyfy(detail[key])

def debugCase(db, caseId, impersonate=False):

	processIds = idsProcessos(db)
	processNames = {v:k for k,v in processIds.items()}

	with db.cursor() as cur :
		def query(aQuery, **vars):
			cur.execute(aQuery, vars)
			return nsList(cur)

		def queryOne(aQuery, **vars):
			many = query(aQuery, **vars)
			if len(many) is not 1:
				warn("S'esperava un sol registre i s'han trobat {}\n"
					"A la query:\n{}\nAmb:\n{}"
					.format(len(many),aQuery,ns(vars).dump()))
			return many[0]

		def queryOneOrEmpty(aQuery, **vars):
			many = query(aQuery, **vars)
			if not many: return ns()
			if len(many) is not 1:
				warn("S'esperava un sol registre o cap i s'han trobat {}\n"
					"A la query:\n{}\nAmb:\n{}"
					.format(len(many),aQuery,ns(vars).dump()))
			return many[0]


		case = queryOneOrEmpty(
			'SELECT * FROM giscedata_switching WHERE id=%(caseid)s',
			caseid = caseId,
			)
		case.process_name = processNames[case.proces_id]
		case.steps = query("""\
			SELECT * FROM giscedata_switching_step_header
			WHERE sw_id=%(caseid)s
			ORDER BY create_date
			""",
			caseid = caseId,
			)
		for step in case.steps :
			maybeSteps = idsPasses(db, processNames[case.proces_id])
			step.details = []
			for maybeStepName, maybeStepId in maybeSteps.items():
				details = query("""\
					SELECT * FROM giscedata_switching_{}
					WHERE header_id=%(id)s
					""".format(maybeStepName.lower()),
					id = step.id
					)
				if not details: continue
				if impersonate:
					for detail in details:
						impersonatePersonalData(detail)
				step._stepname = maybeStepName
				step.details += details
			if not step.details :
				warn("No s'han trobat detalls del pas")
			if len(step.details)>1:
				warn("Més d'un detall pel pas")
			step.details = step.details[0]
		case.case = queryOne("""\
			SELECT * FROM crm_case
			WHERE id=%(crm)s
			""",
			crm=case.case_id,
			)
		case.polissa = queryOne("""\
			SELECT * FROM giscedata_polissa
			WHERE id=%(polissaid)s
			""",
			polissaid=case.cups_polissa_id,
			)
	if impersonate:
		impersonatePersonalData(case.polissa)
		case.polissa.observacions = 'bla bla bla'
	
	return case

import b2btest
import unittest

def skipIfNoPersonalDataAccess():
	if os.access('b2bdata/personal', os.X_OK):
		return lambda x:x
	return unittest.skip("Requires confidential b2b data")


@unittest.skipIf(config is None, "No dbconfig.py file found")
class DebugCase_Test(b2btest.TestCase) :

	def __str__(self): return self.id()

	def setUp(self):
		from dbconfig import psycopg as config
		self.db = psycopg2.connect(**config)

	def tearDown(self):
		self.db.rollback()
		self.db.close()

	def test_anonyfy(self):
		self.assertEqual(
			anonyfy("Perico"),
			'P....o')

	def test_anonify_multiword(self):
		self.assertEqual(
			anonyfy("Perico de los Palotes"),
			'P....o de l.s P.....s')

	def _test_debugCase(self,caseid,description) :
		result = debugCase(self.db, caseid, impersonate=True)
		self.assertBack2Back(result.dump(), 'debugcase-{}.yaml'.format(description))

#	@skipIfNoPersonalDataAccess()
	def test_debugCase_activated_c1(self) :
		self._test_debugCase('5342', 'activated_c1')

#	@skipIfNoPersonalDataAccess()
	def test_debugCase_cancelled_c1(self) :
		self._test_debugCase('7492', 'cancelled_c1')

#	@skipIfNoPersonalDataAccess()
	def test_debugCase_rejected_c1(self) :
		self._test_debugCase('8091', 'rejected_c1')

#	@skipIfNoPersonalDataAccess()
	def test_debugCase_dropout_c1_differentCreationAndActivationMonth(self) :
		self._test_debugCase('8166', 'dropout_c1_differentCreationAndActivationMonth')

#	@skipIfNoPersonalDataAccess()
	def test_debugCase_activated_b1(self) :
		self._test_debugCase('15235', 'activated_b1')

#	@skipIfNoPersonalDataAccess()
	def test_debugCase_cancelpending_c2(self) :
		self._test_debugCase('4149', 'cancelpending_c2')



if __name__ == '__main__' :
	from dbconfig import psycopg as config
	db = psycopg2.connect(**config)
	if len(sys.argv) is not 2:
		fail("Usage: {} <caseid>".format(sys.argv[0]))
	print debugCase(db, sys.argv[1]).dump()







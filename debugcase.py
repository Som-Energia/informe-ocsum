#!/usr/bin/env python
#-*- encoding: utf-8 -*-

from dbutils import *
from dbqueries import loadQuery, idsProcessos, idsPasses
import psycopg2
import sys
import os
from namespace import namespace as ns
from consolemsg import warn, fail



def debugCase(db, caseId):
	processIds = idsProcessos(db)
	processNames = {v:k for k,v in processIds.items()}
	with db.cursor() as cur :
		cur.execute('SELECT * FROM giscedata_switching WHERE id=%(caseid)s',
			dict(
				caseid = caseId,
			))
		case = nsList(cur)
		if not case: return ns()
		case = case[0]
		case.process_name = processNames[case.proces_id]
		cur.execute("""\
			SELECT * FROM giscedata_switching_step_header
			WHERE sw_id=%(caseid)s
			ORDER BY create_date
			""",
			dict(
				caseid = caseId,
			))
		case.steps = nsList(cur)
		for step in case.steps :
			maybeSteps = idsPasses(db, processNames[case.proces_id])
			step.details = []
			for maybeStepName, maybeStepId in maybeSteps.items():
				cur.execute(
					'SELECT * FROM giscedata_switching_{} WHERE header_id=%(id)s'.format(maybeStepName.lower()),
					dict(
						id = step.id,
					))
				details = nsList(cur)
				if not details: continue
				step._stepname = maybeStepName
				step.details += details
			if not step.details :
				warn("No s'han trobat detalls del pas")
			if len(step.details)>1:
				warn("Més d'un detall pel pas")
			step.details = step.details[0]
	
	return case

import b2btest
import unittest

@unittest.skipIf(config is None, "No dbconfig.py file found")
class DebugCase_Test(b2btest.TestCase) :

	def setUp(self):
		from dbconfig import psycopg as config
		self.db = psycopg2.connect(**config)

	def tearDown(self):
		self.db.rollback()
		self.db.close()

	def _test_debugCase(self,caseid,description) :
		result = debugCase(self.db, caseid)
		self.assertBack2Back(result.dump(), 'personal/debugcase-{}.yaml'.format(description))

#	@unittest.skipIf(not os.access('personal', os.X_OK), "Requires confidential b2b data")
	def test_debugCase_activated_c1(self) :
		self._test_debugCase('5342', 'activated_c1')

#	@unittest.skipIf(not os.access('personal', os.X_OK), "Requires confidential b2b data")
	def test_debugCase_cancelled_c1(self) :
		self._test_debugCase('7492', 'cancelled_c1')

#	@unittest.skipIf(not os.access('personal', os.X_OK), "Requires confidential b2b data")
	def test_debugCase_rejected_c1(self) :
		self._test_debugCase('8091', 'rejected_c1')



if __name__ == '__main__' :
	from dbconfig import psycopg as config
	db = psycopg2.connect(**config)
	if len(sys.argv) is not 2:
		fail("Usage: {} <caseid>".format(sys.argv[0]))
	print debugCase(db, sys.argv[1]).dump()







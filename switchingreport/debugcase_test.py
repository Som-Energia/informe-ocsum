#!/usr/bin/env python
#-*- encoding: utf-8 -*-

from dbutils import *
import psycopg2
import os

from debugcase import debugCase, anonyfy

try: import dbconfig as config
except ImportError: config=None

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





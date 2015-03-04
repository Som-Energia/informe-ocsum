import unittest


class TestCase(unittest.TestCase) :
	"""
	This class is yet quite adhoc for single case.
	Paths should be configurable aswell as the way of composing
	data file names.
	"""

	def assertBack2Back(self, result, testId) :
		def write(result) :
			with open(resultfilename,'w') as resultfile:
				resultfile.write(result)
		import os

		resultfilename = 'b2bdata/{}-result{}'.format(*os.path.splitext(testId))
		expectedfilename = 'b2bdata/{}-expected{}'.format(*os.path.splitext(testId))

		if os.access(resultfilename, os.F_OK) :
			os.unlink(resultfilename)

		try :
			with open(expectedfilename) as expectedfile:
				expected=expectedfile.read()
		except IOError as e:
			write(result)
			raise AssertionError(
				"No expectation, accept with: mv {} {}".format(
					resultfilename, expectedfilename))

		self.maxDiff = None
		try:
			self.assertMultiLineEqual(expected, result)
		except AssertionError:
			import sys
			print(
				"Back-to-back results differ, accept with: mv {} {}".format(
					resultfilename, expectedfilename))
			write(result)
			raise

def assertXmlEqual(self, got, want):
	from lxml.doctestcompare import LXMLOutputChecker
	from doctest import Example

	checker = LXMLOutputChecker()
	if checker.check_output(want, got, 0):
		return
	message = checker.output_difference(Example("", want), got, 0)
	raise AssertionError(message)

import unittest
unittest.TestCase.assertXmlEqual = assertXmlEqual



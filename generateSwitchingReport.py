#!/usr/bin/env python
#-*- encoding: utf-8 -*-

from switchingreport import (
	geneateCsv,
	reportName,
	fullGenerate,
	)

from consolemsg import step


if __name__ == '__main__' :
	import sys
	import os
	if '--test' in sys.argv:
		sys.argv.remove('--test')
		unittest.TestCase.__str__ = unittest.TestCase.id
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

	import datetime
	inici=datetime.date(args.year,args.month,1)
	try:
		final=datetime.date(args.year,args.month+1,1)
	except ValueError:
		final=datetime.date(args.year+1,1,1)

	if args.csv:
		geneateCsv(inici, final, args.year, args.month)
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




# vim: ts=4 sw=4 noet

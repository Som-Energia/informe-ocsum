# SwitchingReport

***WARNING: WIP! THIS MODULE IS SUBJECT OF CURRENT ACTIVE DEVELOPMENT AND IT IS NOT FUNCTIONAL YET***


[![Build Status](https://travis-ci.org/Som-Energia/informe-ocsum.png?branch=master)](https://travis-ci.org/Som-Energia/informe-ocsum)
[![Coverage Status](https://coveralls.io/repos/Som-Energia/informe-ocsum/badge.png?branch=master)](https://coveralls.io/r/Som-Energia/informe-ocsum?branch=master)
[![Issue Stats](http://www.issuestats.com/github/Som-Energia/informe-ocsum/badge/pr)](http://www.issuestats.com/github/Som-Energia/informe-ocsum)
[![Issue Stats](http://www.issuestats.com/github/Som-Energia/informe-ocsum/badge/issue)](http://www.issuestats.com/github/Som-Energia/informe-ocsum)


This Python module can be used by electricity retailers
to generate the report Spanish regulators require about
consumer retailer switching.

Format definition is avaiable at: 
[CNMC website](http://cambiodecomercializador.cnmc.es/),
but requires user and password.
An old but open version is still available at the former regulator,
[OCSUM](http://www.ocsum.es/index.php/doc/formatos).

The module is coupled to the data model developed by Gisce
on the top of their OpenERP fork for electricity market agents.


## Requirements

### Native dependencies on Debian/Ubuntu

	$ sudo apt-get install libpq-dev libyaml-dev libxml2-dev libxslt1-dev

### Python dependencies

	$ pip install -r requirements.txt

## Testing

In order to run the database b2b tests you need to create a `dbconfig` module.
It should contain a dicctionary with the database configuration, in
a global var named `psycodb`.

If you don't have it, database dependant tests will be skipped.

To run the tests:

	$ python2 setup.py test




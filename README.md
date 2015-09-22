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
The script does not need the OpenERP, just access to the database model.
 
## Requirements

### Native dependencies on Debian/Ubuntu

	$ sudo apt-get install libpq-dev libyaml-dev libxml2-dev libxslt1-dev

### Python dependencies

	$ pip install -r requirements.txt

## Running

### Configuration

Configuration is made via a module named `dbconfig.py` in the root folder of the code.
It should contain a dicctionary with the database configuration, in
a global var named `psycodb`. That dictionary should contain the
configuration parameters for a psycoql database object constructor.
The distributed file 'dbconfig.py.example` can be used as example.

### Command line interface

To run it as script two parameters are required: year and month.

```bash
./switchingreport.py 2015 6
```

This will generate an XML file with the required nomenclator.

You can use the `--csv` option to generate a set of `CSV`
to easily inspect the collected data in a spread shet program.
You can import them in LibreOffice by setting tags as separators
and UTF8 as character encoding.

## Testing

### Running

To run the tests:

```bash
$ python2 setup.py test
```

### Enabling database dependant tests

If you don't have a `dbconfig.py` file, database dependant tests will be skipped.
But the rest of the test will be run.

### Accepting or discarding modification to b2b data

Two scripts are provided:

- `acceptb2b.sh`: to turn all `result` data in to the `expected`
- `discardb2b.sh`: to discard all `result` data and stich with the current `expected`

Before accepting new data, take a close look at the changes.

## Concepts

The report is sent for a period of time, usually from the first of a month till the last day.
Requests are counted, not contracts, 
so that three requests of the same contract count as three, not one.

- **Sent requests ("Solicitudes enviadas"):**
    - Those sent during the period regardless its resolution.

- **Unanswered requests ("Solicitudes pendientes de respuesta"):**
    - Those without neither rejection nor acceptation answer from the distributor at the end of the period.
    - Warning: This could include requests started on previous periods.

- **Accepted requests ("Solicitudes aceptadas"):**
    - Those accepted within the period
    - Warning: This could include requests started on previous periods.

- **Rejected requests ("Solicitudes rechazadas"):**
    - Those rejected within the period
        - Direct rejection: C1/2,A3:02 with rejection flag
        - After failed field intervention: C1/2,A3:04
    - Includes also rejections because of field intervention after acceptation
    - Does not include rejections because of the format
    - Warning: This could include requests started on previous periods.


- **Unactivated requests ("Solicitudes pendientes de activacion"):**
    - Those accepted but not activated at the end of the period
    - Warning: This could include requests **send or accepted** on previous periods.
    - Plazo: desde la carga de la solicitud a final de mes

- **Activated requests ("Solicitudes activadas"):**
    - Those with activation date (C1/2:05,07) during the period
    - Warning: Not the reception date of C1/2:05,07, but the indicated activation date
    - Warning: This may include request **sent or accepted** on previous periods.
    - Includes later repositionated requests
    - Plazo: desde la carga de la solicitud a la fecha indicada para la activación

- **Cancelled requests ("Solicitudes anuladas"):**
    - Those cancelled (C1/2:09 o A1:06)
    - Warning: the step that counts is the acceptation of the cancellation (C1/2:09) not the cancellation request (C1/2:08)
    - TODO: Ensure that 

- **Repositioned requests ("Solicitudes reposicionadas"):**
    - Those the outgoing 


- **Response times:**
	- Legal start: 01 step file is uploaded
	- Actual start: 01 step file is created
	- Legal end: FechaSolicitud field of the 02 step or the end of the period if not answered
	- Actual end: idem but if legacy distri, used the policy activation
	- Applies to: accepted, rejected, unanswered
- **Activation times:**
	- Legal start: User formalizes the request
	- Actual start: 01 stepfile is created
	- End: 05 step in DatosActivacion/Fecha or the end of the period if not activated
	- Actual end: idem but if legacy distri, used the policy activation
	- Applies to: activated, unactivated

### Invariants

* `unanswered_old + sent == accepted + rejected (no intervention) + unanswered + cancelled`
* `unactivated_old + accepted == activated + rejected (after intervetion) + cancelled + unactivated`



## Casos extraños

- 9982: tiene un 02 con campos a null, rebutjat i proritat 5 (acceptat)
- 6538: prioritat 5 ok a 2015-02
- 7132: prioritat 5 ok a 2015-03
- 5335: Polissa sense cups associat (al formulari estava)
- 4149: Cancelacio no resposta

- There is no a3-07 cancelation step to test in real data


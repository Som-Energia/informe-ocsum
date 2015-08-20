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


## Concepts

The report is sent for a period consisting in a month.
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
        - Direct rejection: C1/2:02 with rejection flag
        - After failed field intervention: C1/2:04
    - Includes also rejections because of field intervention after acceptation
    - Does not include rejections because of the format
    - Warning: This could include requests started on previous periods.

- **Unactivated requests ("Solicitudes pendientes de activacion"):**
    - Those accepted but not activated at the end of the period
    - Warning: This could include requests **send or accepted** on previous periods.
    - Plazo: desde la carga de la solicitud a final de mes

- **Activated requests ("Solicitudes activadas"):**
    - Those with activation date (C1/2:04) during the period
    - Warning: Not the reception date of C1/2:04, but the indicated activation date
    - Warning: This may include request **sent or accepted** on previous periods.
    - Includes later repositionated requests
    - Plazo: desde la carga de la solicitud a la fecha indicada para la activación

- **Cancelled requests ("Solicitudes anuladas"):**
    - Those cancelled (C1/2:09 o A1:06)
    - Warning: the step that counts is the acceptation of the cancellation (C1/2:09) not the cancellation request (C1/2:08)

- **Repositioned requests ("Solicitudes reposicionadas"):**
    - Those the outgoing 







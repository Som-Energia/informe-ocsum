- ~~Rejected requests details~~
- ~~Activation pending request details~~
- ~~Activated requests details~~
- ~~Short queries (by distributor, fare, in/out retailer...)~~
	- ~~Sent requests count~~
	- ~~Cancelled requests count~~
	- ~~Repositioned requests count~~ -> Not to be implemented
	- ~~Outgoing clients count~~
	- ~~Unpaid contracts count~~ -> Not to be implemented
- ~~Skip db base test when no bd config available~~
- ~~Setup a requirements.txt file~~
- ~~Add setup.py to run tests~~
- ~~Setup travisCI~~

- To review:
	- Distributor code is not the one required
	- Rejected includes MotivoRechazo field
	- Rejected segregates by MotivoRechazo
	- NumIncidencias accounted in unactivated requests, how many have any C2_03 A3_03
	- NumIncidencias accounted in activated requests, how many have any C2_03 A3_03
	- TM en accepted, rejected, activated
	- TipoRetraso en unanswered, accepted, rejected, unactivated, activated
	- Segregation by TipoCambio
	- Segregation by TipoPunto
	- Take TarifaATR from case to be unaffected by later changes
- Deployment
	- Separate tests from code
	- Add setup.py to run install
	- ~~CLI frontend~~
	- GUI frontend
	- OERP frontend
	- Windows setup
- To investigate:
	- ~~When TipoCambio has to be 'C4' instead of 'C3'~~ -> C4 is a direct to the market (not a comercializer change)
	- ~~Where to obtain `TipoPunto`~~ -> In end-consumers it depends on which range, defined by BOE max power gets
	- ~~`Comer_entrante` really always the emitter retailer?~~ -> yep
	- ~~`Comer_saliente` really always unknown (0)?~~ -> yep
	- Check all the date intervals
	- Pick a date for managed request with no C2 (by email) (written down as 'case.priority')
	- Current fare could be different from the one at the reported period
	- Rejected marked as priority '4' are not taken into account
	- ~~`NumIncidencias` set to 0 in Activated and ActivationPending

- Things we accept we are doing wrong, eventually fixed
	- sent: we are using create_date but it should be the upload date, not the same





- Distributor code is not the one required
- ~~Rejected requests details~~
- Activation pending request details
- ~~Activated requests details~~
- Short queries (by distributor, fare, in/out retailer...)
	- ~~Sent requests count~~
	- ~~Cancelled requests count~~
	- Repositioned requests count
	- ~~Outgoing clients count~~
	- Unpaid contracts count
- ~~Skip db base test when no bd config available~~
- ~~Setup a requirements.txt file~~
- ~~Add setup.py to run tests~~
- ~~Setup travisCI~~
- Separate tests from code
- Add setup.py to run install
- CLI frontend
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
	- `NumIncidencias` set to 0 in Activated and ActivationPending






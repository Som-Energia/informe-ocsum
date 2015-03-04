- Distributor code is not the one required
- Rejected requests details
- Activation pending request details
- Activated requests details
- Short queries (by distributor, fare, in/out retailer...)
	- Sent requests count
	- Cancelled requests count
	- Repositioned requests count
	- Outgoing clients count
	- Unpaid contracts count
- Separate tests from code
- Add setup.py to run tests and install
- Skip db base test when no bd config available
- Setup travisCI
- To investigate:
	- When TipoCambio has to be 'C4' instead of 'C3'
	- Where to obtain `TipoPunto`
	- `Comer_entrante` really always the emitter retailer?
	- `Comer_saliente` really always unknown (0)?
	- Check all the date intervals
	- Pick a date for managed request with no C2 (by email) (written down as 'case.priority')
	- Current fare could be different from the one at the reported period





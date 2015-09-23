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

- ~~Segregation by TipoCambio~~
- ~~Segregation by TipoPunto~~
- ~~BUG: unactivated, activated are not filtered and got dupes~~
- ~~Taking into account 07 steps (activation with intervention) in unactivated~~
- ~~Taking into account 07 steps (activation with intervention) in activated~~
- NumIncidencias accounted in unactivated requests, count any C2_03 A3_03
- NumIncidencias accounted in activated requests, count any C2_03 A3_03
- No a3_07 cancelled cases to test in real data
- No a3_02 rejected cases to test in real data
- No a3_02 accepted cases to test in real data (some in december 2012)
- No a3_05 activated cases to test in real data
- BUG: unactivated not properly selecting cases, example: the A3 4379 in 2014-02
- BUG: Reject reason None


- To review:
	- ~~Take TarifaATR from case to be unaffected by later changes~~
	- ~~Distributor code is not the one required (using ref1 instead new ref2)~~
	- ~~Rejected includes MotivoRechazo field~~
	- ~~Rejected segregates by MotivoRechazo~~
	- TM en accepted, rejected, activated
	- TipoRetraso en unanswered, accepted, rejected, unactivated, activated
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
	- Pick a date for managed request with no Cn-02 (by email) (written down as 'case.priority')
	- Current fare could be different from the one at the reported period
	- Rejected marked as priority '4' are not taken into account
	- ~~`NumIncidencias` set to 0 in Activated and ActivationPending~~

- Things we accept we are doing wrong, eventually fixed
	- sent: we are using create_date but it should be the upload date, not the same





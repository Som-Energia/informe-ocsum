# TODO's

- [ ] use shared b2btest framework
- [x] Separate tests from code
- [x] turn it into a module
- [x] install scripts
- [ ] merge into dataexports
- [x] crontab script
- [ ] Script launcher integration


- [ ] NumIncidencias accounted in unactivated requests, count any C2_03 A3_03
- [ ] NumIncidencias accounted in activated requests, count any C2_03 A3_03
- [ ] No a3_07 cancelled cases to test in real data
- [ ] No a3_02 rejected cases to test in real data
- [ ] No a3_02 accepted cases to test in real data (some in december 2012)
- [ ] No a3_05 activated cases to test in real data
- [ ] BUG: unactivated not properly selecting cases, example: the A3 4379 in 2014-02
- [ ] Cas 41010: associar-ho al CUPS
- [ ] Cas 6817: associar-ho al CUPS
- [ ] Cas 38114: why reject reason None
- [ ] Cas 41907: why reject reason None

- [ ] To review:
	- [ ] TM en accepted, rejected, activated
	- [ ] TipoRetraso en unanswered, accepted, rejected, unactivated, activated
- [ ] To investigate:
	- [ ] Check all the date intervals
	- [ ] Pick a date for managed request with no Cn-02 (by email) (written down as 'case.priority')
	- [ ] Current fare could be different from the one at the reported period
	- [ ] Rejected marked as priority '4' are not taken into account

- [ ] Things we accept we are doing wrong, eventually fixed
	- [ ] sent: we are using create_date but it should be the upload date, not the same

# DONE's

- [x] Take TarifaATR from case to be unaffected by later changes
- [x] Distributor code is not the one required (using ref1 instead new ref2)
- [x] Rejected includes MotivoRechazo field
- [x] Rejected segregates by MotivoRechazo
- [+] Deployment
	- [x] Add setup.py to run install
	- [x] CLI frontend
- [x] When TipoCambio has to be 'C4' instead of 'C3' -> C4 is a direct to the market (not a comercializer change)
- [x] Where to obtain `TipoPunto` -> In end-consumers it depends on which range, defined by BOE max power gets
- [x] `Comer_entrante` really always the emitter retailer? -> yep
- [x] `Comer_saliente` really always unknown (0)? -> yep
- [x] `NumIncidencias` set to 0 in Activated and ActivationPending
- [x] Rejected requests details
- [x] Activation pending request details
- [x] Activated requests details
- [x] Short queries (by distributor, fare, in/out retailer...)
	- [x] Sent requests count
	- [x] Cancelled requests count
	- [x] Repositioned requests count -> Not to be implemented
	- [x] Outgoing clients count
	- [x] Unpaid contracts count -> Not to be implemented
- [x] Skip db base test when no bd config available
- [x] Setup a requirements.txt file
- [x] Add setup.py to run tests
- [x] Setup travisCI

- [x] Segregation by TipoCambio
- [x] Segregation by TipoPunto
- [x] BUG: unactivated, activated are not filtered and got dupes
- [x] Taking into account 07 steps (activation with intervention) in unactivated
- [x] Taking into account 07 steps (activation with intervention) in activated
- [x] BUG: Reject reason None -> 99
- [x] unactivated filters cancelled c1_09
- [x] unactivated filters cancelled c2_09
- [x] unactivated filters cancelled a3_07


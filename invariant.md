# Classes:

## Events to check if happened during the period

- sent:
	- C1_01, C2_01, A3_01
- accepted:
	- C1_02, C2_02, A3_02 (rebuig==false)
- rejected:
	- C1_02, C2_02, A3_02 (rebuig==true)
	- after_intervention: C2_04, A3_04
- cancelled:
	- C1_09, C2_09, A3_07 (rebuig==false)
	- Segun los 'criterios de conteo' la <FechaSolicitud> del Cn_09 o del A3_06 se usara para determinar en que mes se cuenta.
- activated:
	- C1_05, C2_05, A3_05
	- after_intervention: C1_07, C2_07
- repositionated
	- TODO
- dropout:
	- C1_06, C2_06, B1_05
- unpaid
	- B1_05 Motivo 04
	- Nota: Motivos
		- 01: Cese de actividad
		- 02: Fin de contrato sin concurrencia (a comer de referencia)
		- 03: Suspension por impago
		- 04: Baja por impago



## To check at the end of the month

- unanswered: sent (at any time) but not rejected or accepted
- unactivated: accepted (at any time) but not activated or cancelled
- active: activated (at any time) but not dropout or repositionated

# Invariants:

* unanswered_old + sent == accepted + rejected (no intervention) + unanswered + cancelled
* unactivated_old + accepted == activated + rejected (after intervetion) + cancelled + unactivated
* active_old + activated = active + dropout + repositionated



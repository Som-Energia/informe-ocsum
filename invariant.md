# Classes:

## Events to check if happened during the period

- sent:
	- C1_01, C2_01, A1_01
- accepted:
	- C1_02, C2_02, A1_02 (rebuig==false)
- rejected:
	- C1_02, C2_02, A1_02 (rebuig==true)
	- after_intervention: C1_04, C2_04
- cancelled:
	- C1_08, C2_08, A1_?? (rebuig==false)
- activated:
	- C1_05, C2_05, A1_??
	- after_intervention: C1_07, C2_07
- repositionated
- dropout:
	- C1_06, C2_06, A1_??
- unpaid



## To check at the end of the month

- unanswered: sent (at any time) but not rejected or accepted
- unactivated: accepted (at any time) but not activated or cancelled
- active: activated (at any time) but not dropout or repositionated

# Invariants:

* unanswered_old + sent == accepted + rejected + unanswered + cancelled (some)
* unactivated_old + accepted == activated + cancelled (some) + unactivated
* active_old + activated = active + dropout + repositionated



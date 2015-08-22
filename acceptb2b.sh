#!/bin/bash

found=''
for a in b2bdata/*-result.* b2bdata/personal/*-result.*; do 
	[ -e $a ] || continue
	mv $a ${a/-result/-expected}
	found="$found $a"
	echo "Accepting ${a}..."
done
if [ -z found ]
then
	echo "No s'han trobat resultats pendents d'acceptar"
fi



#!/bin/bash

found=''
for a in b2bdata/*-result.*; do 
	[ -e "$a" ] || continue
	rm "$a"
	found="$found $a"
	echo "Discarding ${a}..."
done
if [ -z found ]
then
	echo "No s'han trobat resultats pendents d'acceptar"
fi



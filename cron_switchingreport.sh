#!/bin/bash

die() {
    [ -z "$1" ] || echo -e '\033[34;1mError: '$*'\033[0m' >&2
    exit -1
}
step() {
    echo -e '\033[34;1m:: '$*'\033[0m'
}

scriptpath=$(dirname $(readlink -f "$0"))
cd "$scriptpath"
 
TOOPTIONS=$(
while read r
do
    [ -n "$r" ] &&  echo "--to $r"
done < recipients-switchingreport
)
date


today=$(date -I)
IFS='-' read -r year month day <<< "$today" # split date
lastMonthEnd=$(date -I -d "$year-$month-01 - 1 day") # last day of last month
IFS='-' read -r year month day <<< "${1:-$lastMonthEnd}" # split date

step "Generant resum del $year-$month-$day"

./atr_switchingreport $year $month --csv || die No he pogut generar els CSV
./atr_switchingreport $year $month || die No he pogut generar el XML


allreports=(SI_R2-???_E_${year}${month}_??.xml)
lastReport=${allreports[-1]}

MONTHS=("" Enero Febrero Marzo Abril Mayo Junio Julio Agosto Septiembre Octubre Noviembre Diciembre)

monthname=${MONTHS[$month]}


step "Enviant resultats..."

TEXTOK="
# Informe de Cambios de comercializadora para SomEnergia $monthname de $year

Les adjuntamos el informe de cambios de comercializador correspondiente al
mes de ${MONTHS[$month]} de $year para la comercializadora \"Som Energia SCCL\".

Un saludo.
"

emili.py \
    --subject "SomEnergia SCCL, informe canvios de comercializador, $year-$month" \
    $TOOPTIONS \
    --from sistemes@somenergia.coop \
    --replyto david.garcia@somenergia.coop \
    --config $scriptpath/dbconfig.py \
    --format md \
    --style somenergia.css \
    $lastReport \
    report-$year$month-*.csv \
    --body "$TEXTOK" \
    || die


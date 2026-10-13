#!/usr/bin/env bash
# Bun Shell não suporta arrays/funções Bash; o wrapper preserva ambas as invocações.


# Três posições encaminham as duas flags e um argumento inválido extra.
bash "$(dirname "$0")/reproduce.bash" "$1" "$2" "$3"





# O sufixo isola projetos e containers dedicados, impedindo compartilhamento entre execuções.

# COMPOSE_PROJECT_NAME substitui apenas o projeto principal.
# Sem sufixo explícito, o nome do projeto também isola containers dedicados.

# Portas não derivam do sufixo; cada execução precisa escolher portas distintas.

jwp_repro_validate_token() {
  local value="$1" label="$2"
  if [[ ! "$value" =~ ^[a-z0-9]([a-z0-9-]{0,24})?$ ]]; then
    echo "invalid $label: $value (use [a-z0-9-], at most 25 characters)" >&2
    return 2
  fi
}

jwp_repro_resolve_names() {
  JWP_REPRO_SUFFIX="${JWP_REPRO_SUFFIX:-}"
  if [ -n "$JWP_REPRO_SUFFIX" ]; then
    jwp_repro_validate_token "$JWP_REPRO_SUFFIX" JWP_REPRO_SUFFIX || return 2
  fi
  if [ -n "${COMPOSE_PROJECT_NAME:-}" ]; then
    jwp_repro_validate_token "$COMPOSE_PROJECT_NAME" COMPOSE_PROJECT_NAME || return 2
    JWP_REPRO_PROJECT="$COMPOSE_PROJECT_NAME"
  else
    JWP_REPRO_PROJECT="jwp-repro${JWP_REPRO_SUFFIX:+-$JWP_REPRO_SUFFIX}"
  fi
  local dedicated="$JWP_REPRO_SUFFIX"
  if [ -z "$dedicated" ] && [ "$JWP_REPRO_PROJECT" != "jwp-repro" ]; then
    dedicated="$JWP_REPRO_PROJECT"
  fi
  JWP_REPRO_DEDICATED_SUFFIX="$dedicated"
  JWP_REPRO_OBS_PROJECT="jwp-observability-repro${dedicated:+-$dedicated}"
  if [ "$JWP_REPRO_OBS_PROJECT" = "$JWP_REPRO_PROJECT" ]; then
    JWP_REPRO_OBS_PROJECT="${JWP_REPRO_PROJECT}-observability"
  fi
}


jwp_repro_container() {
  local base="$1"
  if [ -n "${JWP_REPRO_DEDICATED_SUFFIX:-}" ]; then
    printf 'jwp-%s-%s-repro' "$base" "$JWP_REPRO_DEDICATED_SUFFIX"
  else
    printf 'jwp-%s-repro' "$base"
  fi
}

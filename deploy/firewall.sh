#!/usr/bin/env bash
# Only the tailnet may reach the published proxy port. Docker-published ports bypass ufw,
# so the drop lives in DOCKER-USER, which Docker evaluates before its own forwarding rules.
# Idempotent; run by marketagg-firewall.service after Docker starts.
set -euo pipefail
PORT="${MARKETAGG_PUBLIC_PORT:-8000}"
RULE=(-p tcp -m conntrack --ctdir ORIGINAL --ctorigdstport "$PORT" ! -i tailscale0 -j DROP)
iptables -N DOCKER-USER 2>/dev/null || true
iptables -C DOCKER-USER "${RULE[@]}" 2>/dev/null || iptables -I DOCKER-USER 1 "${RULE[@]}"

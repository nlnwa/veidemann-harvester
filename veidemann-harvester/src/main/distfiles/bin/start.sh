#!/usr/bin/env sh

iptables -t nat -A OUTPUT -p tcp -m owner --uid-owner root --dport 443 -j REDIRECT --to-port 8080
iptables -t nat -A OUTPUT -p tcp -m owner --uid-owner root --dport 80 -j REDIRECT --to-port 8080

iptables -t nat --list

mkdir /data/certificates
chmod 777 /data/certificates
#exec /veidemann/bin/veidemann-harvester
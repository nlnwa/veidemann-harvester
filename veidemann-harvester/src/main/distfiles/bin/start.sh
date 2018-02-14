#!/usr/bin/env sh

if [ -n "$CACHE_HOST" ]; then
    CACHE_IP=$(nslookup ${CACHE_HOST} | grep "Address 1" | cut -f3 -d' ')
    echo "Adding cache route to ${CACHE_HOST} (${CACHE_IP})"
fi

su-exec root update-ca-certificates

mkdir -p /workdir/certificates
cp /ca-certificates/cache-selfsignedCA.crt /workdir/certificates/cache-selfsignedCA.crt
chmod -R 777 /workdir/certificates


su-exec operator /veidemann/bin/veidemann-harvester
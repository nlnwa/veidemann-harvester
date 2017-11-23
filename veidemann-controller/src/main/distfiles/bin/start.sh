#!/bin/bash

# Eventually import extra CA certificate
if [ -e /veidemann-controller/tls/tls.crt ]; then
    openssl x509 -in /veidemann-controller/tls/tls.crt -inform pem -out /veidemann-controller/config/ca.der -outform der
    keytool -importcert -trustcacerts -noprompt -alias startssl -keystore $JAVA_HOME/jre/lib/security/cacerts -storepass changeit -file /veidemann-controller/config/ca.der
fi

exec /veidemann-controller/bin/veidemann-controller
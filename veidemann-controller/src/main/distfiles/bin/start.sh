#!/bin/sh

# Eventually import extra trusted CA certificates
if [ -e /veidemann/trustedca/tls.crt ]; then
    openssl x509 -in /veidemann/trustedca/tls.crt -inform pem -out /veidemann/config/ca.der -outform der
    keytool -importcert -trustcacerts -noprompt -alias startssl -keystore $JAVA_HOME/jre/lib/security/cacerts -storepass changeit -file /veidemann/config/ca.der
fi

exec /veidemann/bin/veidemann-controller
#!/usr/bin/env bash

keystore_path="src/main/resources/ds.jks"
pass='password'
echo 'Deleting previous keystore...'
rm -fv ${keystore_path}
echo 'Generating new keystore...'
keytool -genkey -noprompt \
  -keyalg RSA -alias ds \
  -keystore ${keystore_path} \
  -storepass ${pass} \
  -validity 3650 -keysize 4096 -storetype pkcs12 \
  -dname "CN=localhost, O=Morgan Stanley, C=USA"

echo 'Exporting key...'
keytool -noprompt -importkeystore -srckeystore ${keystore_path} \
  -srcalias ds -srcstoretype jks \
  -destkeystore ds.p12  -deststoretype pkcs12 -storepass ${pass} < <(echo -e "${pass}\n${pass}\n")

echo 'Converting key'
openssl pkcs12 -in ds.p12 -out ds.pem -password pass:${pass} -passout pass:
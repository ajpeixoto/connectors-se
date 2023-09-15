# Apache 2 server container for some tck/HTTPClient tests

## How run
Build the image:
```bash
$ docker build -t httptest .
```
Add `httptest` name in your local `/etc/hosts` file:
```bash
$ echo "127.0.0.1 httptest" >> /etc/hosts
```  
Run the docker image
```bash
$ docker run --add-host "httptest:127.0.0.1" -d --name apache2-container -e TZ=UTC -p 8080:80 -p 44300:443 -p 45300:453  httptest
```

Then you can connect to https://httptest:44300/

## Certificates
To enable HTTPS I have followed this page: https://linuxhint.com/enable-https-apache-web-server/

And I have generated certificates with:
```bash
$ openssl req -new -newkey rsa:4096 -x509 -sha256 -days 365 -nodes -out apache.crt -keyout apache.key
```
And set `httptest` as `Common Name`.

## Endpoints
- https://httptest:44300/ : the default welcome page
- https://httptest:44300/geologists.json : a json document with a list of geologists

## Authentications
I have followed this documentation to add support of basic authentication: https://httpd.apache.org/docs/2.4/fr/howto/auth.html

A password file has been created in `/usr/local/apache2/passwds/passwords` via la commande:
```bash
$ htpasswd -c /usr/local/apache2/passwds/passwords peter
```
Added users:
- `peter` / `aze123#`
- `john` / `abcd123`

For this URL https://httptest:45300/digest_authent.json
the user is john:abcde

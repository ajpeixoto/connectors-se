FROM ubuntu/apache2

RUN apt-get update -yq \
&& apt-get -y install vim \
&& apt-get -y install openssl \
&& apt-get -y install apache2-doc \
&& a2enmod ssl \
&& a2enmod rewrite \
&& a2enmod auth_digest\
&& mkdir /etc/apache2/certs/ \
&& mkdir -p /usr/local/apache2/passwds/ \
&& mkdir -p /var/www/basic_authent/ \
&& mkdir -p /var/www/digest_authent/

ADD apache2.conf /etc/apache2/
ADD certs/apache.crt /etc/apache2/certs/
ADD certs/apache.key /etc/apache2/certs/

ADD passwords /usr/local/apache2/passwds/
ADD .htaccess_basic /var/www/basic_authent/
ADD .htaccess_digest /var/www/digest_authent/

RUN mv /var/www/basic_authent/.htaccess_basic /var/www/basic_authent/.htaccess \
&& mv /var/www/digest_authent/.htaccess_digest /var/www/digest_authent/.htaccess

ADD geologists.json /var/www/html/
ADD basic_authent.json /var/www/basic_authent
ADD digest_authent.json /var/www/digest_authent

EXPOSE 80 443 453
CMD ["apache2-foreground"]

FROM ubuntu

RUN apt-get update && \
    apt-get install -y curl libjson-perl

CMD ["/usr/bin/perl", "/opt/listener.pl"]
ADD . /opt

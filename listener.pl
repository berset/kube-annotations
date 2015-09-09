#!/usr/bin/perl -w

use JSON;
use Data::Dumper;

# Read
my $kube_master = $ENV{'KUBERNETES_MASTER'}//'127.0.0.1';
my $kube_port = $ENV{'KUBERNETES_MASTER_PORT'}//'8080';
my $ns = 'default';
# Write
my $influxdb = 'cadvisor-influxdb';
my $db = 'k8s';
my $user = 'root';
my $pass = 'root';
# State
my $rv = '';
sub curl {
    my $resourceVersion = shift;
    my $watch = shift;
    my $cmd = sprintf("curl -m 3 -s -k 'http://%s:%d/api/v1/namespaces/%s/replicationcontrollers?pretty=false&resourceVersion=%s&watch=%s'"
            , $kube_master
            , $kube_port
            , $ns
            , $resourceVersion
            , $watch
            );
    my $data = `$cmd`;
    return $data;
}

my $data = curl($rv,'false');
my $initial_list = decode_json($data);
#print Dumper $initial_list;
$rv = $initial_list->{'metadata'}->{'resourceVersion'};

my $rcs = {};

for my $item (@{$initial_list->{'items'}}) {
    $rcs->{$item->{'metadata'}->{'name'}} = $item;
}

while (1) {
    my $data = curl($rv,'true');
    if ($data ne '') {
        my @events = ();
        for my $row (split(/\n/, $data)) {
            my $event = decode_json($row);
            if ($event->{'type'} eq 'ERROR') {
                if ($event->{'object'}->{'message'} =~ /401: .* \[([0-9]+)\]/) {
                    $rv = $1;
                    #print "update rv: ",$rv,"\n";
                }
            } elsif ($event->{'type'} eq 'MODIFIED') {
                eval {
                    my $rc_name = $event->{'object'}->{'metadata'}->{'name'};
                    my $old = $rcs->{$rc_name};
                    my $new = $event->{'object'};
                    #print Dumper $event;
                    #print ("---------------vs--------------------\n");
                    #print Dumper $old;
                    my @cols = ('message');
                    my @points = (
                        sprintf("scale %s to: %d current: %d",
                            $rc_name,
                            $new->{'spec'}->{'replicas'},
                            $old->{'status'}->{'replicas'}
                            )
                        );
                    # TODO add labels
                    for my $k (keys %{$new->{'metadata'}->{'labels'}}) {
                        push @cols, $k;
                        push @points, $new->{'metadata'}->{'labels'}->{$k};
                    }
                    my $influxdb_message = {
                        name => 'events',
                        columns => \@cols,
                        points => [\@points]
                        };
                    push @events, $influxdb_message;
                    $rcs->{$rc_name} = $new;
                };
                if ($@) {
                    print "bad data: '",$row,"'\n";
                    die;
                }
                $rv = $event->{'object'}->{'metadata'}->{'resourceVersion'};
            } elsif ($event->{'type'} eq 'DELETED') {
                eval {
                    my $rc_name = $event->{'object'}->{'metadata'}->{'name'};
                    my $old = $rcs->{$rc_name};
                    my $new = $event->{'object'};
                    my @cols = ('message');
                    my @points = (
                        sprintf("deleted %s",
                            $rc_name
                            )
                        );
                    # TODO add labels
                    for my $k (keys %{$new->{'metadata'}->{'labels'}}) {
                        push @cols, $k;
                        push @points, $new->{'metadata'}->{'labels'}->{$k};
                    }
                    my $influxdb_message = {
                        name => 'events',
                        columns => \@cols,
                        points => [\@points]
                        };
                    push @events, $influxdb_message;
                    delete $rcs->{$rc_name};
                };
                if ($@) {
                    print "bad data: '",$row,"'\n";
                    die;
                }
                $rv = $event->{'object'}->{'metadata'}->{'resourceVersion'};
            } elsif ($event->{'type'} eq 'ADDED') {
                eval {
                    my $rc_name = $event->{'object'}->{'metadata'}->{'name'};
                    my $old = $rcs->{$rc_name};
                    my $new = $event->{'object'};
                    my @cols = ('message');
                    my @points = (
                        sprintf("added %s",
                            $rc_name
                            )
                        );
                    # TODO add labels
                    for my $k (keys %{$new->{'metadata'}->{'labels'}}) {
                        push @cols, $k;
                        push @points, $new->{'metadata'}->{'labels'}->{$k};
                    }
                    my $influxdb_message = {
                        name => 'events',
                        columns => \@cols,
                        points => [\@points]
                        };
                    push @events, $influxdb_message;
                    $rcs->{$rc_name} = $new;
                };
                if ($@) {
                    print "bad data: '",$row,"'\n";
                    die;
                }
                $rv = $event->{'object'}->{'metadata'}->{'resourceVersion'};
            } else {
                die Dumper $event;
            }
        }

        if (scalar @events > 0) {
            my $url  = sprintf(q{http://%s:8086/db/%s/series?u=%s&p=%s}
                    , $influxdb
                    , $db
                    , $user
                    , $pass
                    );
            system('/usr/bin/curl', '-X', 'POST'
                , '-d', encode_json(\@events)
                , $url) or print STDERR 'upload of data points failed';
        }
    }
    print STDERR ".";
}

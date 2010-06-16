use strict;
use warnings;
use Test::More;

package test::host;
use Moose; with 'Net::Riak::Role::Hosts';

package main;

my $test = test::host->new();
is scalar @{$test->host}, 1, 'got one host';

ok my $host = $test->get_host, 'got host';
is $host, 'http://127.0.0.1:8098', 'host is ok';

$test = test::host->new(host => ['http://10.0.0.40', 'http://10.0.0.41']);
is scalar @{$test->host}, 2, 'got two hosts';
ok $host = $test->get_host, 'got host';

done_testing;

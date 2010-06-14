use strict;
use warnings;
use Test::More;

use Net::Riak::Client;
use Net::Riak::Bucket;
use Net::Riak::Link;

my $client = Net::Riak::Client->new();
my $bucket = Net::Riak::Bucket->new(name => 'foo', client => $client);

ok my $link = Net::Riak::Link->new(bucket => $bucket), 'link created';

my $header = $link->to_link_header($client);

is $header, '</riak/foo/_>; riaktag="foo"', 'generate valid link string';

done_testing;

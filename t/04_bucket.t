use strict;
use warnings;
use Test::More;

use Net::Riak::Bucket;
use Net::Riak::Client;

my $client = Net::Riak::Client->new;
ok my $bucket = Net::Riak::Bucket->new(name => 'foo', client => $client),
  'client created';

done_testing;

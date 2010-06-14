use strict;
use warnings;
use Test::More;

use JSON;
use HTTP::Response;

use Net::Riak::Bucket;
use Net::Riak::Client;
use Net::Riak::Object;

my $client = Net::Riak::Client->new();
my $bucket = Net::Riak::Bucket->new(name => 'foo', client => $client);

ok my $object =
  Net::Riak::Object->new(key => 'bar', bucket => $bucket, client => $client),
  'object bar created';

my $response = HTTP::Response->new(400);

ok !$object->exists, 'object don\'t exists';

eval {
    $object->populate($response, [200]);
};

like $@, qr/Expected status 200, received 400/, "can't populate with a 400";

my $value = {value => 1};

$response = HTTP::Response->new(200);
$response->content(JSON::encode_json($value));

$object->populate($response, [200]);

ok $object->exists, 'object exists';

is_deeply $value, $object->data, 'got same data';

is $object->status, 200, 'last http code is 200';

done_testing;

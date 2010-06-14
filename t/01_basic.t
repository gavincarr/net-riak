use strict;
use warnings;
use Test::More;
use Net::Riak;
use YAML::Syck;

BEGIN {
  unless ($ENV{RELEASE_TESTING}) {
    require Test::More;
    Test::More::plan(skip_all => 'these tests are for release candidate testing');
  }
}

my $host = 'http://localhost:8098';
my $bucket_name = 'test4';
my $bucket_multi = 'multiBucket2';

# is alive
{
    ok my $client = Net::Riak->new(), 'client created';
    ok $client->is_alive, 'riak is alive';
}

# store and get
{
    ok my $client = Net::Riak->new(), 'client created';
    ok my $bucket = $client->bucket($bucket_name), 'got bucket test';
    my $content = [int(rand(100))];
    ok my $obj = $bucket->new_object('foo', $content),
      'created a new riak object';
    ok $obj->store,       'store object foo';
    is $obj->status,      200, 'valid status';
    is $obj->key,         'foo', 'valid key';
    is_deeply $obj->data, $content, 'valid content';
}

# missing object
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj    = $bucket->get("missing");
    ok !$obj->data, 'no data';
}

# delete object
{
    my $client  = Net::Riak->new();
    my $bucket  = $client->bucket($bucket_name);
    my $content = [int(rand(100))];
    my $obj     = $bucket->new_object('foo', $content);
    ok $obj->store, 'object is stored';
    $obj = $bucket->get('foo');
    ok $obj->exists, 'object exists';
    $obj->delete;
    $obj->load;
    ok !$obj->exists, "object don't exists anymore";
}

# test set bucket properties
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    $bucket->allow_multiples('True');
    my $res = $bucket->allow_multiples;
    $bucket->n_val(3);
    is $bucket->n_val, 3, 'n_val is set to 3';
    $bucket->set_properties({allow_mult => "False", "n_val" => 2});
    ok !$bucket->allow_multiples, "don't allow multiple anymore";
    is $bucket->n_val, 2, 'n_val is set to 2';
}

# test siblings
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_multi);
    $bucket->allow_multiples(1);
    ok $bucket->allow_multiples, 'multiples set to 1';
    my $obj = $bucket->get('foo');
    $obj->delete;
    for(1..5) {
        my $client = Net::Riak->new();
        my $bucket = $client->bucket($bucket_multi);
        $obj = $bucket->new_object('foo', [int(rand(100))]);
        $obj->store;
    }
    # check we got 5 siblings
    ok $obj->has_siblings, 'object has siblings';
    $obj = $bucket->get('foo');
    my $siblings_count = $obj->get_siblings;
    is $siblings_count, 5, 'got 5 siblings';
    # test set/get
    my @siblings = $obj->siblings;
    my $obj3 = $obj->sibling(3);
    is_deeply $obj3->data, $obj->sibling(3)->data;
    $obj3 = $obj->sibling(3);
    $obj3->store;
    $obj->load;
    is_deeply $obj->data, $obj3->data;
    $obj->delete;
}

# test js source map
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj    = $bucket->new_object('foo', [2])->store;
    my $result =
      $client->add($bucket_name, 'foo')
      ->map("function (v) {return [JSON.parse(v.values[0].data)];}")->run;
    is_deeply $result, [[2]], 'got valid result';
}

# XXX javascript named map
# {
#     my $client     = Net::Riak->new();
#     my $bucket     = $client->bucket($bucket_name);
#     my $obj        = $bucket->new_object('foo', [2])->store;
#     my $result = $client->add("bucket", "foo")->map("Riak.mapValuesJson")->run;
#     use YAML; warn Dump $result;
#     is_deeply $result, [[2]], 'got valid result';
# }

# javascript source map reduce
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj    = $bucket->new_object('foo', [2])->store;
    $obj = $bucket->new_object('bar', [3])->store;
    $bucket->new_object('baz', [4])->store;
    my $result =
      $client->add($bucket_name, "foo")->add($bucket_name, "bar")
      ->add($bucket_name, "baz")->map("function (v) { return [1]; }")
      ->reduce("function (v) { return [v.length]; }")->run;
    is $result->[0], 3, "success map reduce";
}

# javascript named map reduce
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj    = $bucket->new_object("foo", [2])->store;
    $obj = $bucket->new_object("bar", [3])->store;
    $obj = $bucket->new_object("baz", [4])->store;
    my $result =
      $client->add($bucket_name, "foo")->add($bucket_name, "bar")
      ->add($bucket_name, "baz")->map("Riak.mapValuesJson")
      ->reduce("Riak.reduceSum")->run();
    ok $result->[0];
}

# javascript bucket map reduce
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket("bucket_".int(rand(10)));
    $bucket->new_object("foo", [2])->store;
    $bucket->new_object("bar", [3])->store;
    $bucket->new_object("baz", [4])->store;
    my $result =
      $client->add($bucket->name)->map("Riak.mapValuesJson")
      ->reduce("Riak.reduceSum")->run;
    ok $result->[0];
}

# javascript map reduce from object
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    $bucket->new_object("foo", [2])->store;
    my $obj = $bucket->get("foo");
    my $result = $obj->map("Riak.mapValuesJson")->run;
    is_deeply $result->[0], [2], 'valid content';
}

# store and get links
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object("foo", [2]);
    my $obj1 = $bucket->new_object("foo1", {test => 1})->store;
    my $obj2 = $bucket->new_object("foo2", {test => 2})->store;
    my $obj3 = $bucket->new_object("foo3", {test => 3})->store;
    $obj->add_link($obj1);
    $obj->add_link($obj2, "tag");
    $obj->add_link($obj3, "tag2!@&");
    $obj->store;
    $obj = $bucket->get("foo");
    my $count = $obj->count_links;
    is $count, 3, 'got 3 links';
}

# link walking
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj    = $bucket->new_object("foo", [2]);
    my $obj1   = $bucket->new_object("foo1", {test => 1})->store;
    my $obj2   = $bucket->new_object("foo2", {test => 2})->store;
    my $obj3   = $bucket->new_object("foo3", {test => 3})->store;
    $obj->add_link($obj1)->add_link($obj2, "tag")->add_link($obj3, "tag2!@&");
    $obj->store;
    $obj = $bucket->get("foo");
    my $results = $obj->link($bucket_name)->run();
    is scalar @$results, 3, 'got 3 links via links walking';
    $results = $obj->link($bucket_name, 'tag')->run;
    is scalar @$results, 1, 'got one link via link walking';
}

done_testing;

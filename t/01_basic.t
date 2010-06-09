use strict;
use warnings;
use Test::More;
use Net::Riak;
use YAML::Syck;

my $host = 'http://localhost:8098';
my $bucket_name = 'test4';
my $bucket_multi = 'multiBucket1';

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
    ok !$obj->exists;
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
    ok !$bucket->allow_multiples;
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
        my $rand = int(rand(100));
        $obj = $bucket->new_object('foo', [$rand]);
        $obj->store;
    }
    # my $siblings_count = $obj->get_siblings;
    # is $siblings_count, 5, 'got 5 siblings';
    # my $obj3 = $obj->sibling(3);
    # XXX FIXME
    # $obj3 = $obj3->sibling(3);
    # $obj3->store;
    # $obj->reload;
    # is_deeply $obj3->data, $obj->data;
    # $obj->delete;
}

# test js source map
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object('foo', [2]);
    $obj->store;
    my $map_reduce = $client->add($bucket_name, 'foo');
    $map_reduce->map("function (v) {return [JSON.parse(v.values[0].data)];}");
    my $result = $map_reduce->run();
    is_deeply $result, [[2]], 'got valid result';
}

# javascript named map
{
    my $client     = Net::Riak->new();
    my $bucket     = $client->bucket($bucket_name);
    my $obj        = $bucket->new_object('foo', [2]);
    my $map_reduce = $client->add("bucket", "foo");
    $map_reduce->map("Riak.mapValuesJson");
    my $result = $map_reduce->run;
    use YAML::Syck;
    warn Dump $result;
}

# javascript source map reduce
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object('foo', [2]);
    $obj->store;
    $obj = $bucket->new_object('bar', [3]);
    $obj->store;
    $bucket->new_object('baz', [4]);
    $obj->store;
    my $map_reduce = $client->add($bucket_name, "foo");
    $map_reduce->add($bucket_name, "bar");
    $map_reduce->add($bucket_name, "baz");
    $map_reduce->map("function (v) { return [1]; }");
    $map_reduce->reduce("function (v) { return [v.length]; }");
    my $result = $map_reduce->run;
    is $result->[0], 3, "success map reduce";
}

# javascript named map reduce
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object("foo", [2]);
    $obj->store;
    $obj = $bucket->new_object("bar", [3]);
    $obj->store;
    $obj = $bucket->new_object("baz", [4]);
    $obj->store;
    my $map_reduce = $client->add($bucket_name, "foo");
    $map_reduce->add($bucket_name, "bar");
    $map_reduce->add($bucket_name, "baz");
    $map_reduce->map("Riak.mapValuesJson");
    $map_reduce->reduce("Riak.reduceSum");
    my $result = $map_reduce->run();
#    is $result->[0], 243; # ????
}

# javascript bucket map reduce
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object("foo", [2]);
    $obj->store;
    $obj = $bucket->new_object("bar", [3]);
    $obj->store;
    $obj = $bucket->new_object("baz", [4]);
    $obj->store;
    my $map_reduce = $client->add($bucket->name);
    $map_reduce->map("Riak.mapValuesJson");
    $map_reduce->reduce("Riak.reduceSum");
    my $result = $map_reduce->run;
    ok 1, "ici";
#    is $result->[0], 243;
}

# javascript map reduce from object
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object("foo", [2]);
    $obj->store;
    $obj = $bucket->get("foo");
    my $map_reduce = $obj->map("Riak.mapValuesJson");
    my $result = $map_reduce->run();
    is_deeply $result->[0], [2];
}

# store and get links
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object("foo", [2]);
    my $obj1 = $bucket->new_object("foo1", {test => 1});
    $obj1->store;
    my $obj2 = $bucket->new_object("foo2", {test => 2});
    $obj2->store;
    my $obj3 = $bucket->new_object("foo3", {test => 3});
    $obj3->store;
    $obj->add_link($obj1);
    $obj->add_link($obj2, "tag");
    $obj->add_link($obj3, "tag2!@&");
    $obj->store;
    $obj = $bucket->get("foo");
    my $mr = $obj->link("bucket");
    my $results = $mr->run();
    # XXX fixme !!
    use YAML::Syck; warn Dump $results;
}

# link walking
{
    my $client = Net::Riak->new();
    my $bucket = $client->bucket($bucket_name);
    my $obj = $bucket->new_object("foo", [2]);
    my $obj1 = $bucket->new_object("foo1", {test => 1});
    $obj1->store;
    my $obj2 = $bucket->new_object("foo2", {test => 2});
    $obj2->store;
    my $obj3 = $bucket->new_object("foo3", {test => 3});
    $obj3->store;
    $obj->add_link($obj1);
    $obj->add_link($obj2, "tag");
    $obj->add_link($obj3, "tag2!@&");
    $obj->store;
    $obj = $bucket->get("foo");
    my $mr = $obj->link("bucket");
    my $results = $mr->run();
    use YAML::Syck; warn Dump $results;
}

done_testing;

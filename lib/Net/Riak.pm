package Net::Riak;

# ABSTRACT: Interface to Riak

use Moose;

use Net::Riak::Client;
use Net::Riak::Bucket;

with 'Net::Riak::Role::MapReduce';

has client => (
    is       => 'rw',
    isa      => 'Net::Riak::Client',
    required => 1,
    handles  => [qw/request useragent is_alive/]
);

sub BUILDARGS {
    my ($class, %args) = @_;
    my $client = Net::Riak::Client->new(%args);
    $args{client} = $client;
    \%args;
}

sub bucket {
    my ($self, $name) = @_;
    my $bucket = Net::Riak::Bucket->new(name => $name, client => $self->client);
    $bucket;
}

1;

=head1 SYNOPSIS

    my $client = Net::Riak->new(host => 'http://10.0.0.40:8098');
    my $bucket = $client->bucket('blog');
    my $obj    = $bucket->new_object('new_post', {title => 'foo', content => 'bar'});
    $obj->store;

    my $obj = $bucket->get('new_post');

=head1 DESCRIPTION

=head2 ATTRIBUTES

=over 4

=item B<host>

URL of the node (default 'http://127.0.0.1:8098'). If your ring is composed with more than one node, you can configure the client to hit more than one host, instead of hitting always the same node. For this, you can do one of the following:

=over 4

=item B<all nodes equals>

    my $riak = Net::Riak->new(
        host => [
            'http://10.0.0.40:8098',
            'http://10.0.0.41:8098'
        ]
    );

=item B<give weight to nodes>

    my $riak = Net::Riak->new(
        host => [
            {node => 'http://10.0.0.40:8098', weight => '0.2'},
            {node => 'http://10.0.0.41:8098', weight => '0.8'}
        ]
    );

=back

Now, when a request is made, a node is picked at random, according to weight.

=item B<prefix>

Interface prefix (default 'riak')

=item B<mapred_prefix>

MapReduce prefix (default 'mapred')

=item B<r>

R value setting for this client (default 2)

=item B<w>

W value setting for this client (default 2)

=item B<dw>

DW value setting for this client (default 2)

=item B<client_id>

client_id for this client

=back

=head2 METHODS

=over 4

=item bucket

    my $bucket = $client->bucket($name);

Get the bucket by the specified name. Since buckets always exist, this will always return a L<Net::Riak::Bucket>

=item is_alive

    if (!$client->is_alive) {
        ...
    }

Check if the Riak server for this client is alive

=item add

    my $map_reduce = $client->add('bucket_name', 'key');

Start assembling a Map/Reduce operation

=item link

    my $map_reduce = $client->link();

Start assembling a Map/Reduce operation

=item map

    my $map_reduce = $client->add('bucket_name', 'key')->map("function ...");

Start assembling a Map/Reduce operation

=item reduce

    my $map_reduce = $client->add(..)->map(..)->reduce("function ...");

Start assembling a Map/Reduce operation

=back

=cut


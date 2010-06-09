package Net::Riak;

# ABSTRACT: Interface to Riak

use Moose;
use MIME::Base64;

use Net::Riak::Bucket;
use Net::Riak::MapReduce;

with qw/
  Net::Riak::Role::REST
  Net::Riak::Role::UserAgent
  /;

has host =>
  (is => 'rw', isa => 'Str', coerce => 1, default => 'http://127.0.0.1:8098');
has prefix        => (is => 'rw', isa => 'Str', default    => 'riak');
has mapred_prefix => (is => 'rw', isa => 'Str', default    => 'mapred');
has r             => (is => 'rw', isa => 'Int', default    => 2);
has w             => (is => 'rw', isa => 'Int', default    => 2);
has dw            => (is => 'rw', isa => 'Int', default    => 2);
has client_id     => (is => 'rw', isa => 'Str', lazy_build => 1,);

sub _build_client_id {
    "perl_net_riak" . encode_base64(int(rand(10737411824)), '');
}

sub bucket {
    my ($self, $name) = @_;
    my $bucket = Net::Riak::Bucket->new(name => $name, client => $self);
    $bucket;
}

sub is_alive {
    my $self     = shift;
    my $request  = $self->request('GET', ['ping']);
    my $response = $self->useragent->request($request);
    $response->is_success ? return 1 : return 0;
}

sub add {
    my ($self, @args) = @_;
    my $mr = Net::Riak::MapReduce->new(client => $self);
    $mr->add(@args);
    $mr;
}

sub link {
    my ($self, @args) = @_;
    my $mr = Net::Riak::MapReduce->new(client => $self);
    $mr->link(@args);
    $mr;
}

sub map {
    my ($self, @args) = @_;
    my $mr = Net::Riak::MapReduce->new(client => $self);
    $mr->mapd(@args);
    $mr;
}

sub reduce {
    my ($self, @args) = @_;
    my $mr = Net::Riak::MapReduce->new(client => $self);
    $mr->reduce(@args);
    $mr;
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

Hostname or IP address (default 'http://127.0.0.1:8098')

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

=method bucket

    my $bucket = $client->bucket($name);

Get the bucket by the specified name. Since buckets always exist, this will always return a L<Net::Riak::Bucket>

=method is_alive

Check if the Riak server for this client is alive

=method add

Start assembling a Map/Reduce operation

=method link

Start assembling a Map/Reduce operation

=method map

Start assembling a Map/Reduce operation

=method reduce

Start assembling a Map/Reduce operation

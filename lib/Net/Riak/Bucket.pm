package Net::Riak::Bucket;

# ABSTRACT: Access and change information about a Riak bucket

use JSON;
use Moose;
use Carp;
use Net::Riak::Object;

with 'Net::Riak::Role::Replica' => {keys => [qw/r w dw/]};
with 'Net::Riak::Role::Base' =>
  {classes => [{name => 'client', required => 1}]};

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);
has content_type => (
    is      => 'rw',
    isa     => 'Str',
    default => 'application/json'
);

sub n_val {
    my $self = shift;
    if (my $val = shift) {
        $self->set_property('n_val', $val);
    }
    else {
        $self->get_property('n_val');
    }
}

sub allow_multiples {
    my $self = shift;

    if (my $val = shift) {
        my $bool = ($val == 1 ? JSON::true : JSON::false);
        $self->set_property('allow_mult', $bool);
    }
    else {
        return $self->get_property('allow_mult');
    }
}

sub get_keys {
    my ($self, $params) = @_;
    my $key_mode = delete($params->{stream}) ? 'stream' : 'true';
    $params = { props => 'false', keys => $key_mode, %$params };
    my $properties = $self->get_properties($params);
    return $properties->{keys};
}

sub get {
    my ($self, $key, $r) = @_;
    my $obj = Net::Riak::Object->new(
        client => $self->client,
        bucket => $self,
        key    => $key
    );
    $r ||= $self->r;
    $obj->load($r);
    $obj;
}

sub set_property {
    my ($self, $key, $value) = @_;
    $self->set_properties({$key => $value});
}

sub get_property {
    my ($self, $key, $params) = @_;
    my $props = $self->get_properties($params);
    return $props->{props}->{$key};
}

sub get_properties {
    my ($self, $params) = @_;

    # Callbacks require stream mode
    $params->{keys}  = 'stream' if $params->{cb};

    $params->{props} = 'true'  unless exists $params->{props};
    $params->{keys}  = 'false' unless exists $params->{keys};

    my $request =
      $self->client->request('GET', [$self->client->prefix, $self->name],
        $params);

    my $response = $self->client->useragent->request($request);

    if (!$response->is_success) {
        die "Error getting bucket properties: " . $response->status_line . "\n";
    }

    if ($params->{keys} ne 'stream') {
        return JSON::decode_json($response->content);
    }

    # In streaming mode, aggregate keys from the multiple returned chunk objects
    else {
        my $json = JSON->new;
        my $props = $json->incr_parse($response->content);
        if ($params->{cb}) {
            while (defined(my $obj = $json->incr_parse)) {
                $params->{cb}->($_) foreach @{$obj->{keys}};
            }
            return %$props ? { props => $props } : {};
        }
        else {
            my @keys = map { $_->{keys} && ref $_->{keys} eq 'ARRAY' ? @{$_->{keys}} : () }
                $json->incr_parse;
            return { props => $props, keys => \@keys };
        }
    }
}

sub set_properties {
    my ($self, $props) = @_;

    my $request = $self->client->request('PUT', [$self->client->prefix, $self->name]);
    $request->header('Content-Type' => $self->content_type);
    $request->content(JSON::encode_json({props => $props}));
    my $response = $self->client->useragent->request($request);

    if (!$response->is_success) {
        die "Error setting bucket properties: " . $response->status_line . "\n";
    }
}

sub new_object {
    my ($self, $key, $data, @args) = @_;
    my $object = Net::Riak::Object->new(
        key    => $key,
        data   => $data,
        bucket => $self,
        client => $self->client,
        @args,
    );
    $object;
}

1;

=head1 SYNOPSIS

    my $client = Net::Riak->new(...);
    my $bucket = $client->bucket('foo');

    # retrieve an existing object
    my $obj1 = $bucket->get('foo');

    # create/store a new object
    my $obj2 = $bucket->new_object('foo2', {...});
    $object->store;

=head1 DESCRIPTION

The L<Net::Riak::Bucket> object allows you to access and change information about a Riak bucket, and provides methods to create or retrieve objects within the bucket.

=head2 ATTRIBUTES

=over 4

=item B<name>

    my $name = $bucket->name;

Get the bucket name

=item B<r>

    my $r_value = $bucket->r;

R value setting for this client (default 2)

=item B<w>

    my $w_value = $bucket->w;

W value setting for this client (default 2)

=item B<dw>

    my $dw_value = $bucket->dw;

DW value setting for this client (default 2)

=back

=head2 METHODS

=over 4

=item new_object

    my $obj = $bucket->new_object($key, $data, @args);

Create a new L<Net::Riak::Object> object. Additional Object constructor arguments can be passed after $data. If $data is a reference and no explicit Object content_type is given in @args, the data will be serialised and stored as JSON.

=item get

    my $obj = $bucket->get($key, [$r]);

Retrieve an object from Riak.

=item n_val

    my $n_val = $bucket->n_val;

Get/set the N-value for this bucket, which is the number of replicas that will be written of each object in the bucket. Set this once before you write any data to the bucket, and never change it again, otherwise unpredictable things could happen. This should only be used if you know what you are doing.

=item allow_multiples

    $bucket->allow_multiples(1|0);

If set to True, then writes with conflicting data will be stored and returned to the client. This situation can be detected by calling has_siblings() and get_siblings(). This should only be used if you know what you are doing.

=item get_keys

    my $keys = $bucket->get_keys;
    my $keys = $bucket->get_keys($args);

Return an arrayref of the list of keys for a bucket. Optionally takes a hashref of named parameters. Supported parameters are:

=over 4

=item stream => 1

Uses key streaming mode to fetch the list of keys, which may be faster for large keyspaces.

=item cb => sub { }

A callback subroutine to be called for each key found (passed in as the only parameter). get_keys() returns nothing in callback mode.

=back

=item set_property

    $bucket->set_property({n_val => 2});

Set a bucket property. This should only be used if you know what you are doing.

=item get_property

    my $prop = $bucket->get_property('n_val');

Retrieve a bucket property.

=item set_properties

Set multiple bucket properties in one call. This should only be used if you know what you are doing.

=item get_properties

Retrieve an associative array of all bucket properties, containing 'props' and 'keys' elements.

Accepts a hashref of parameters. Supported parameters are:

=over 4

=item props => 'true'|'false'

Whether to return bucket properties. Defaults to 'true' if no parameters are given.

=item keys => 'true'|'false'|'stream'

Whether to return bucket keys. If set to 'stream', uses key streaming mode, which may be faster for large keyspaces.

=item cb => sub { }

A callback subroutine to be called for each key found (passed in as the only parameter). Implies keys => 'stream'. Keys are omitted from the results hashref in callback mode.

=back

=back

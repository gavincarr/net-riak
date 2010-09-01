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
    my $self = shift;
    my $properties = $self->get_properties({keys => 'true', props => 'false'});
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

    $params->{props} = 'true'  unless exists $params->{props};
    $params->{keys}  = 'false' unless exists $params->{keys};

    my $request =
      $self->client->request('GET', [$self->client->prefix, $self->name],
        $params);

    my $response = $self->client->useragent->request($request);

    my $props = {};
    if ($response->is_success) {
        $props = JSON::decode_json($response->content);
    }
    return $props;
}

sub set_properties {
    my ($self, $props) = @_;

    my $request = $self->client->request('PUT', [$self->client->prefix, $self->name]);
    $request->header('Content-Type' => $self->content_type);
    $request->content(JSON::encode_json({props => $props}));
    my $response = $self->client->useragent->request($request);

    if (!$response->is_success || $response->code != 204) {
        croak "Error setting bucket properties.";
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
    my $object = $bucket->new_object('foo', {...});
    $object->store;
    $object->get('foo2');

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

=method new_object

    my $obj = $bucket->new_object($key, $data);

Create a new L<Net::Riak::Object> object that will be stored as JSON.

=method get

    my $obj = $bucket->get($key, [$r]);

Retrieve a JSON-encoded object from Riak

=method n_val

    my $n_val = $bucket->n_val;

Get/set the N-value for this bucket, which is the number of replicas that will be written of each object in the bucket. Set this once before you write any data to the bucket, and never change it again, otherwise unpredictable things could happen. This should only be used if you know what you are doing.

=method allow_multiples

    $bucket->allow_multiples(1|0);

If set to True, then writes with conflicting data will be stored and returned to the client. This situation can be detected by calling has_siblings() and get_siblings(). This should only be used if you know what you are doing.

=method get_keys

    my $keys = $bucket->get_keys;

Return the list of keys for a bucket

=method set_property

    $bucket->set_property({n_val => 2});

Set a bucket property. This should only be used if you know what you are doing.

=method get_property

    my $prop = $bucket->get_property('n_val');

Retrieve a bucket property.

=method set_properties

Set multiple bucket properties in one call. This should only be used if you know what you are doing.

=method get_properties

Retrieve an associative array of all bucket properties. By default, 'props' is set to true and 'keys' to false. You can change this default:

    my $properties = $bucket->get_properties({keys=>'true'});


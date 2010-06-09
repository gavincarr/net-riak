package Net::Riak::Bucket;

# ABSTRACT: Access and change information about a Riak bucket

use JSON;
use Moose;
use Net::Riak::Object;

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);
has client => (
    is       => 'ro',
    isa      => 'Net::Riak',
    required => 1
);
has content_type => (
    is      => 'rw',
    isa     => 'Str',
    default => 'application/json'
);

has r => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { (shift)->client->r }
);
has w => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { (shift)->client->w }
);
has dw => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { (shift)->client->dw }
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
        $self->set_property('allow_mult', $val);
    }
    else {
        return $self->get_property('allow_mult');
    }
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
    my ($self, $key) = @_;
    my $props = $self->get_properties;
    return $props->{$key};
}

sub get_properties {
    my $self = shift;

    my $params = {props => 'True', keys => 'False'};

    my $request =
      $self->client->request('GET', [$self->client->prefix, $self->name],
        $params);

    my $response = $self->client->useragent->request($request);

    my $props = {};
    if ($response->is_success) {
        $props = JSON::decode_json($response->content);
        $props = $props->{props};
    }
    return $props;
}

sub set_properties {
    my ($self, $props) = @_;

    my $request = $self->client->request('PUT', [$self->client->prefix, $self->name]);
    $request->header('Content-Type' => $self->content_type);
    $request->content(JSON::encode_json({props => $props}));
    my $response = $self->client->useragent->request($request);

    if (!$response->is_success) {
        # XXX
    }

    if ($response->code != 204) {
        # XXX
    }
}

sub new_object {
    my ($self, $key, $data) = @_;
    my $object = Net::Riak::Object->new(
        key    => $key,
        data   => $data,
        bucket => $self,
        client => $self->client
    );
}

1;

=head1 SYNOPSIS

The L<Net::Riak::Bucket> object allows you to access and change information about a Riak bucket, and provides methods to create or retrieve objects within the bucket.

=head1 DESCRIPTION

=head2 ATTRIBUTES

=item B<name>

Get the bucket name

=item B<r>

R value setting for this client (default 2)

=item B<w>

W value setting for this client (default 2)

=item B<dw>

DW value setting for this client (default 2)

=head2 METHODS

=method new_object

Create a new L<Net::Riak::Object> object that will be stored as JSON.

=method get

Retrieve a JSON-encoded object from Riak

=method n_val

Get/set the N-value for this bucket, which is the number of replicas that will be written of each object in the bucket. Set this once before you write any data to the bucket, and never change it again, otherwise unpredictable things could happen. This should only be used if you know what you are doing.

=method allow_multiples

If set to True, then writes with conflicting data will be stored and returned to the client. This situation can be detected by calling has_siblings() and get_siblings(). This should only be used if you know what you are doing.

=method set_property

Set a bucket property. This should only be used if you know what you are doing.

=method get_property

Retrieve a bucket property

=method set_properties

Set multiple bucket properties in one call. This should only be used if you know what you are doing.

=method get_properties

Retrieve an associative array of all bucket properties.

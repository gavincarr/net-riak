package Net::Riak::Object;

# ABSTRACT: holds meta information about a Riak object

use Carp;
use JSON;
use Moose;
use Scalar::Util;
use Net::Riak::Link;

with 'Net::Riak::Role::Replica' => {keys => [qw/r w dw/]};
with 'Net::Riak::Role::Base' => {classes =>
      [{name => 'bucket', required => 1}, {name => 'client', required => 1}]};

has key => (is => 'rw', isa => 'Str', required => 1);
has status       => (is => 'rw', isa => 'Int');
has exists       => (is => 'rw', isa => 'Bool', default => 0,);
has data         => (is => 'rw', isa => 'Any', clearer => '_clear_data');
has vclock       => (is => 'rw', isa => 'Str', predicate => 'has_vclock',);
has content_type => (is => 'rw', isa => 'Str', default => 'application/json');
has _headers     => (is => 'rw', isa => 'HTTP::Response',);
has _jsonize     => (is => 'rw', isa => 'Bool', lazy => 1, default => 1,);
has links => (
    traits     => ['Array'],
    is         => 'rw',
    isa        => 'ArrayRef[Net::Riak::Link]',
    auto_deref => 1,
    lazy       => 1,
    default    => sub { [] },
    handles    => {
        count_links => 'elements',
        append_link => 'push',
        has_links   => 'count',
    },
    clearer => '_clear_links',
);
has siblings => (
    traits     => ['Array'],
    is         => 'rw',
    isa        => 'ArrayRef[Str]',
    auto_deref => 1,
    lazy       => 1,
    default    => sub { [] },
    handles    => {
        get_siblings    => 'elements',
        add_sibling     => 'push',
        count_siblings  => 'count',
        get_sibling     => 'get',
        has_siblings    => 'count',
        has_no_siblings => 'is_empty',
    },
    clearer => '_clear_links',
);

sub store {
    my ($self, $w, $dw) = @_;

    $w  ||= $self->w;
    $dw ||= $self->dw;

    my $params = {returnbody => 'true', w => $w, dw => $dw};

    my $request =
      $self->client->request('PUT',
        [$self->client->prefix, $self->bucket->name, $self->key], $params);

    $request->header('X-Riak-ClientID' => $self->client->client_id);
    $request->header('Content-Type'    => $self->content_type);

    if ($self->has_vclock) {
        $request->header('X-Riak-Vclock' => $self->vclock);
    }

    if ($self->has_links) {
        $request->header('link' => $self->_links_to_header);
    }

    if (ref $self->data && $self->content_type eq 'application/json') {
        $request->content(JSON::encode_json($self->data));
    }
    else {
        $request->content($self->data);
    }

    my $response = $self->client->useragent->request($request);
    $self->populate($response, [200, 300]);
    $self;
}

sub _links_to_header {
    my $self = shift;
    join(', ', map { $_->to_link_header($self->client) } $self->links);
}

sub load {
    my $self = shift;

    my $params = {r => $self->r};

    my $request =
      $self->client->request('GET',
        [$self->client->prefix, $self->bucket->name, $self->key], $params);

    my $response = $self->client->useragent->request($request);
    $self->populate($response, [200, 300, 404]);
    $self;
}

sub delete {
    my ($self, $dw) = @_;

    $dw ||= $self->bucket->dw;
    my $params = {dw => $dw};

    my $request =
      $self->client->request('DELETE',
        [$self->client->prefix, $self->bucket->name, $self->key], $params);

    my $response = $self->client->useragent->request($request);
    $self->populate($response, [204, 404]);
    $self;
}

sub clear {
    my $self = shift;
    $self->_clear_data;
    $self->_clear_links;
    $self->exists(0);
    $self;
}

sub populate {
    my ($self, $http_response, $expected) = @_;

    $self->clear;

    return if (!$http_response);

    my $status = $http_response->code;
    $self->_headers($http_response);
    $self->status($status);

    $self->data($http_response->content);

    if (!grep { $status == $_ } @$expected) {
        croak "Expected status "
          . (join(', ', @$expected))
          . ", received $status";
    }

    if ($status == 404) {
        $self->clear;
        return;
    }

    $self->exists(1);

    if ($http_response->header('link')) {
        $self->_populate_links($http_response->header('link'));
    }

    if ($status == 300) {
        my @siblings = split("\n", $self->data);
        shift @siblings;
        $self->siblings(\@siblings);
    }

    if ($status == 200) {
        $self->content_type($http_response->content_type)
            if $http_response->content_type;
        $self->data(JSON::decode_json($self->data))
            if $self->content_type eq 'application/json';
        $self->vclock($http_response->header('X-Riak-Vclock'));
    }
}

sub _populate_links {
    my ($self, $links) = @_;

    for my $link (split(',', $links)) {
        if ($link
            =~ /\<\/([^\/]+)\/([^\/]+)\/([^\/]+)\>; ?riaktag=\"([^\']+)\"/)
        {
            my $bucket = $2;
            my $key    = $3;
            my $tag    = $4;
            my $l      = Net::Riak::Link->new(
                bucket => Net::Riak::Bucket->new(
                    name   => $bucket,
                    client => $self->client
                ),
                key => $key,
                tag => $tag
            );
            $self->add_link($l);
        }
    }
}

sub sibling {
    my ($self, $id, $r) = @_;
    $r ||= $self->bucket->r;

    my $vtag = $self->get_sibling($id);
    my $params = {r => $r, vtag => $vtag};

    my $request =
      $self->client->request('GET',
        [$self->client->prefix, $self->bucket->name, $self->key], $params);
    my $response = $self->client->useragent->request($request);

    my $obj = Net::Riak::Object->new(
        client => $self->client,
        bucket => $self->bucket,
        key    => $self->key
    );
    $obj->_jsonize($self->_jsonize);
    $obj->populate($response, [200]);
    $obj;
}

sub add_link {
    my ($self, $obj, $tag) = @_;
    my $new_link;
    if (blessed $obj && $obj->isa('Net::Riak::Link')) {
        $new_link = $obj;
    }
    else {
        $new_link = Net::Riak::Link->new(
            bucket => $self->bucket,
            key    => $self->key,
            tag    => $tag || $self->bucket->name,
        );
    }
    $self->remove_link($new_link);
    $self->append_link($new_link);
    $self;
}

sub remove_link {
    my ($self, $obj, $tag) = @_;
    my $new_link;
    if (blessed $obj && $obj->isa('Net::Riak::Link')) {
        $new_link = $obj;
    }
    else {
        $new_link = Net::Riak::Link->new(
            bucket => $self->bucket,
            key    => $self->key,
            tag    => $tag || ''
        );
    }

    # XXX purge links!
}

sub add {
    my ($self, @args) = @_;
    my $map_reduce = Net::Riak::MapReduce->new(client => $self->client);
    $map_reduce->add($self->bucket->name, $self->key);
    $map_reduce->add(@args);
    $map_reduce;
}

sub link {
    my ($self, @args) = @_;
    my $map_reduce = Net::Riak::MapReduce->new(client => $self->client);
    $map_reduce->add($self->bucket->name, $self->key);
    $map_reduce->link(@args);
    $map_reduce;
}

sub map {
    my ($self, @args) = @_;
    my $map_reduce = Net::Riak::MapReduce->new(client => $self->client);
    $map_reduce->add($self->bucket->name, $self->key);
    $map_reduce->map(@args);
    $map_reduce;
}

sub reduce {
    my ($self, @args) = @_;
    my $map_reduce = Net::Riak::MapReduce->new(client => $self->client);
    $map_reduce->add($self->bucket->name, $self->key);
    $map_reduce->reduce(@args);
    $map_reduce;
}

1;

=head1 SYNOPSIS

    my $obj = $bucket->get('foo');


=head1 DESCRIPTION

The L<Net::Riak::Object> holds meta information about a Riak object, plus the object's data.

=head2 ATTRIBUTES

=over 4

=item B<key>

    my $key = $obj->key;

Get the key of this object

=item B<client>

=item B<bucket>

=item B<data>

Get or set the data stored in this object.

=item B<r>

=item B<w>

=item B<dw>

=item B<content_type>

=item B<status>

Get the HTTP status from the last operation on this object.

=item B<links>

Get an array of L<Net::Riak::Link> objects

=item B<exists>

Return true if the object exists, false otherwise.

=item B<siblings>

Return an array of Siblings

=back

=head2 METHODS

=method count_links

Return the number of links

=method append_link

Add a new link

=method get_siblings

Return the number of siblings

=method add_sibling

Add a new sibling

=method count_siblings

=method get_sibling

Return a sibling

=method store

    $obj->store($w, $dw);

Store the object in Riak. When this operation completes, the object could contain new metadata and possibly new data if Riak contains a newer version of the object according to the object's vector clock.

=over 2

=item B<w>

W-value, wait for this many partitions to respond before returning to client.

=item B<dw>

DW-value, wait for this many partitions to confirm the write before returning to client.

=back

=method load

    $obj->load($w);

Reload the object from Riak. When this operation completes, the object could contain new metadata and a new value, if the object was updated in Riak since it was last retrieved.

=over 4

=item B<r>

R-Value, wait for this many partitions to respond before returning to client.

=back

=method delete

    $obj->delete($dw);

Delete this object from Riak.

=over 4

=item B<dw>

DW-value. Wait until this many partitions have deleted the object before responding.

=back

=method clear

    $obj->reset;

Reset this object

=method has_siblings

    if ($obj->has_siblings) { ... }

Return true if this object has siblings

=method has_no_siblings

   if ($obj->has_no_siblings) { ... }

Return true if this object has no siblings

=method populate

Given the output of RiakUtils.http_request and a list of statuses, populate the object. Only for use by the Riak client library.

=method add_link

    $obj->add_link($obj2, "tag");

Add a link to a L<Net::Riak::Object>

=method remove_link

    $obj->remove_link($obj2, "tag");

Remove a link to a L<Net::Riak::Object>

=method add

Start assembling a Map/Reduce operation

=method link

Start assembling a Map/Reduce operation

=method map

Start assembling a Map/Reduce operation

=method reduce

Start assembling a Map/Reduce operation

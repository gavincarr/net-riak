package Net::Riak::Object;

# ABSTRACT: holds meta information about a Riak object

use Carp;
use JSON;
use Moose;
use Scalar::Util;
use Net::Riak::Link;

has key    => (is => 'rw', isa => 'Str',               required => 1);
has client => (is => 'rw', isa => 'Net::Riak',         required => 1);
has bucket => (is => 'rw', isa => 'Net::Riak::Bucket', required => 1);
has data => (is => 'rw', isa => 'Any', clearer => '_clear_data');
has r =>
  (is => 'rw', isa => 'Int', lazy => 1, default => sub { (shift)->client->r });
has w =>
  (is => 'rw', isa => 'Int', lazy => 1, default => sub { (shift)->client->w });
has dw => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => sub { (shift)->client->dw }
);
has content_type => (is => 'rw', isa => 'Str', default => 'application/json');
has status       => (is => 'rw', isa => 'Int');
has links        => (
    traits     => ['Array'],
    is         => 'rw',
    isa        => 'ArrayRef[Net::Riak::Link]',
    auto_deref => 1,
    default    => sub { [] },
    handles    => {
        get_links => 'elements',
        add_links => 'push',
    },
    clearer => '_clear_links',
);
has exists => (
    is => 'rw',
    isa => 'Bool',
    default => 0,
);
has vclock => (
    is => 'rw',
    isa => 'Str',
    predicate => 'has_vclock',
);
has siblings => (
    traits     => ['Array'],
    is         => 'rw',
    isa        => 'ArrayRef[Str]',
    auto_deref => 1,
    lazy       => 1,
    default    => sub { [] },
    handles    => {
        get_siblings   => 'elements',
        add_sibling    => 'push',
        count_siblings => 'count',
        get_sibling    => 'get',
    },
    clearer => '_clear_links',
);

has _headers => (
    is  => 'rw',
    isa => 'HTTP::Response',
);
has _jsonize => (
    is      => 'rw',
    isa     => 'Bool',
    lazy    => 1,
    default => 1,
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
        $request->header('X-Riack-Vclock' => $self->vclock);
    }

    if ($self->_jsonize) {
        $request->content(JSON::encode_json($self->data));
    }
    else {
        $request->content($self->data);
    }

    my $response = $self->client->useragent->request($request);
    $self->populate($response, [200, 300]);
}

sub load {
    my $self = shift;

    my $params = {r => $self->r};

    my $request =
      $self->client->request('GET',
        [$self->client->prefix, $self->bucket->name, $self->key], $params);

    my $response = $self->client->useragent->request($request);
    $self->populate($response, [200, 300, 404]);
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
}

sub clear {
    my $self = shift;
    $self->_clear_data;
    $self->_clear_links;
    $self->exists(0);
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
        $self->populate_links($http_response->header('link'));
    }

    if ($status == 300) {
        my @siblings = split("\n", $self->data);
        shift @siblings;
        $self->siblings(\@siblings);
    }

    if ($status == 200 && $self->_jsonize) {
        $self->data(JSON::decode_json($self->data));
    }
}

sub populate_links {
    my ($self, $links) = @_;

    for my $link (split(',', $links)) {
        if ($link
            =~ /\<\/([^\/]+)\/([^\/]+)\/([^\/]+)\>; ?riaktag=\"([^\']+)\"/)
        {
            my $l = Net::Riak::Link->new($2, $3, $4);
            $self->add_link($link);
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
    return $obj;
}

sub add_link {
    my ($self, $obj, $tag) = @_;
    my $new_link;
    if (blessed $obj && $obj->isa('RiakLink')) {
        $new_link = $obj;
    }
    else {
        $new_link = Net::Riak::Link->new(
            bucket => $self->bucket,
            key    => $self->key,
            tag    => $tag || ''
        );
    }
    $self->remove_link($new_link);
    $self->add_links($new_link);
}

sub remove_link {
    my ($self, $obj, $tag) = @_;
    my $new_link;
    if (blessed $obj && $obj->isa('RiakLink')) {
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

The L<Net::Riak::Object> holds meta information about a Riak object, plus the object's data.

=head1 DESCRIPTION



=head2 ATTRIBUTES

=head2 METHODS

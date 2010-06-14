package Net::Riak::Link;

# ABSTRACT: the riaklink object represents a link from one Riak object to another

use Moose;

has client => (
    is       => 'ro',
    isa      => 'Net::Riak::Client',
    required => 0,
);
has bucket => (
    is       => 'ro',
    isa      => 'Net::Riak::Bucket',
    required => 1,
);
has key => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => '_',
);
has tag => (
    is      => 'rw',
    isa     => 'Str',
    lazy    => 1,
    default => sub {(shift)->bucket->name}
);

sub to_link_header {
    my ($self, $client) = @_;

    my $link = '';
    $link .= '</';
    $link .= $client->prefix . '/';
    $link .= $self->bucket->name . '/';
    $link .= $self->key . '>; riaktag="';
    $link .= $self->tag . '"';
    return $link;
}

1;


package Net::Riak::Role::Replica;

use MooseX::Role::Parameterized;

parameter keys => (
    isa      => 'ArrayRef',
    required => 1,
);

role {
    my $p = shift;

    my $keys = $p->keys;

    foreach my $k (@$keys) {
        has $k => (
            is      => 'rw',
            isa     => 'Int',
            lazy    => 1,
            default => sub { (shift)->client->$k }
        );
    }
};

1;

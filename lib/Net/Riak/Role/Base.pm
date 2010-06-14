package Net::Riak::Role::Base;

use MooseX::Role::Parameterized;

parameter classes => (
    isa      => 'ArrayRef',
    required => 1,
);

role {
    my $p = shift;

    my $attributes = $p->classes;

    foreach my $attr (@$attributes) {
        my $name     = $attr->{name};
        my $required = $attr->{required},
          my $class  = "Net::Riak::" . (ucfirst $name);
        has $name => (
            is       => 'rw',
            isa      => $class,
            required => $required,
        );
    }
};

1;


package Net::Riak::LinkPhase;

use Moose;
use JSON;

has bucket => (is => 'ro', isa => 'Str', required => 1);
has tag    => (is => 'ro', isa => 'Str', required => 1);
has keep   => (is => 'rw', isa => 'JSON::Boolean', required => 1);

sub to_array {
    my $self     = shift;
    my $step_def = {
        bucket => $self->bucket,
        tag    => $self->tag,
        keep   => $self->keep,
    };
    return {link => $step_def};
}

1;

package Net::Riak::Role::UserAgent;

# ABSTRACT: useragent for Net::Riak

use Moose::Role;
use LWP::UserAgent;

has useragent => (
    is => 'rw',
    isa => 'LWP::UserAgent',
    lazy => 1,
    default => sub {
        my $self = shift;
        my $ua = LWP::UserAgent->new;
        $ua->timeout(3);
        $ua;
    }
);

1;

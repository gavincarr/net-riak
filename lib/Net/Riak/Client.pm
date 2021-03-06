package Net::Riak::Client;

use Moose;
use MIME::Base64;

with qw/
  Net::Riak::Role::REST
  Net::Riak::Role::UserAgent
  Net::Riak::Role::Hosts
  /;

has prefix => (
    is      => 'rw',
    isa     => 'Str',
    default => 'riak'
);
has mapred_prefix => (
    is      => 'rw',
    isa     => 'Str',
    default => 'mapred'
);
has [qw/r w dw/] => (
    is      => 'rw',
    isa     => 'Int',
    default => 2
);
has client_id => (
    is         => 'rw',
    isa        => 'Str',
    lazy_build => 1,
);

sub _build_client_id {
    "perl_net_riak" . encode_base64(int(rand(10737411824)), '');
}

sub is_alive {
    my $self     = shift;
    my $request  = $self->request('GET', ['ping']);
    my $response = $self->useragent->request($request);
    $response->is_success ? return 1 : return 0;
}

1;

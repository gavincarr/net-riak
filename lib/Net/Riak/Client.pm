package Net::Riak::Client;

use Moose;
use MIME::Base64;

with qw/Net::Riak::Role::REST Net::Riak::Role::UserAgent/;

has host => (
    is      => 'rw',
    isa     => 'Str',
    default => 'http://127.0.0.1:8098'
);
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
has r => (
    is      => 'rw',
    isa     => 'Int',
    default => 2
);
has w => (
    is      => 'rw',
    isa     => 'Int',
    default => 2
);
has dw => (
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

1;

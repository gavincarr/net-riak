package Net::Riak::Role::REST;

# ABSTRACT: role for REST operations

use URI;
use HTTP::Request;
use Moose::Role;

sub _build_path {
    my ($self, $path) = @_;
    $path = join('/', @$path);
}

sub _build_uri {
    my ($self, $path, $params) = @_;

    my $uri = URI->new($self->host);
    $uri->path($self->_build_path($path));
    $uri->query_form(%$params);
    $uri;
}

sub request {
    my ($self, $method, $path, $params) = @_;
    my $uri = $self->_build_uri($path, $params);
    my $request = HTTP::Request->new($method => $uri);
}

1;

package Net::Riak::MapReduce;

# ABSTRACT: Allows you to build up and run a map/reduce operation on Riak

use JSON;
use Moose;
use Scalar::Util;

use Net::Riak::LinkPhase;
use Net::Riak::MapReducePhase;

with 'Net::Riak::Role::Base' =>
  {classes => [{name => 'client', required => 0}]};

has phases => (
    traits     => ['Array'],
    is         => 'rw',
    isa        => 'ArrayRef[Object]',
    auto_deref => 1,
    lazy       => 1,
    default    => sub { [] },
    handles    => {
        get_phases => 'elements',
        add_phase  => 'push',
        num_phases => 'count',
        get_phase  => 'get',
    },
);
has inputs_bucket => (
    is => 'rw',
    isa => 'Str',
    predicate => 'has_inputs_bucket',
);
has inputs => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'ArrayRef[ArrayRef]',
    handles => {add_input => 'push',},
    default => sub { [] },
);
has input_mode => (
    is        => 'rw',
    isa       => 'Str',
    predicate => 'has_input_mode',
);

sub add {
    my $self = shift;
    my $arg  = shift;

    if (!scalar @_) {
        if (blessed($arg)) {
            $self->add_object($arg);
          } else {
            $self->add_bucket($arg);
        }
    }
    else {
        $self->add_bucket_key_data($arg, @_);
    }
    $self;
}

sub add_object {
    my ($self, $obj) = @_;
    $self->add_bucket_key_data($obj->bucket->name, $obj->key);
}

sub add_bucket_key_data {
    my ($self, $bucket, $key, $data) = @_;
    if ($self->has_input_mode && $self->input_mode eq 'bucket') {
        croak("Already added a bucket, can't add an object");
    }
    else {
        $self->add_input([$bucket, $key, $data]);
    }
}

sub add_bucket {
    my ($self, $bucket) = @_;
    $self->input_mode('bucket');
    $self->inputs_bucket($bucket);
}

sub link {
    my ($self, $bucket, $tag, $keep) = @_;
    $bucket ||= '_';
    $tag    ||= '_';
    $keep   ||= JSON::false;

    $self->add_phase(
        Net::Riak::LinkPhase->new(
            bucket => $bucket,
            tag    => $tag,
            keep   => $keep
        )
    );
}

sub map {
    my ($self, $function, %options) = @_;

    my $map_reduce = Net::Riak::MapReducePhase->new(
        type     => 'map',
        function => $function,
        keep     => $options{keep} || JSON::false,
        arg      => $options{arg} || [],
    );
    $self->add_phase($map_reduce);
    $self;
}

sub reduce {
    my ($self, $function, %options) = @_;

    my $map_reduce = Net::Riak::MapReducePhase->new(
        type     => 'reduce',
        function => $function,
        keep     => $options{keep} || JSON::false,
        arg      => $options{arg} || [],
    );
    $self->add_phase($map_reduce);
    $self;
}

sub run {
    my ($self, $timeout) = @_;

    my $num_phases = $self->num_phases;
    my $keep_flag  = 0;
    my $query      = [];

    my $total_phase = $self->num_phases;
    foreach my $i (0 .. ($total_phase - 1)) {
        my $phase = $self->get_phase($i);
        if ($i == ($total_phase - 1) && !$keep_flag) {
            $phase->keep(JSON::true);
        }
        $keep_flag = 1 if ($phase->{keep}->isa(JSON::true));
        push @$query, $phase->to_array;
    }

    my $inputs;
    if ($self->has_input_mode && $self->input_mode eq 'bucket' && $self->has_inputs_bucket) {
        $inputs = $self->inputs_bucket;
    }else{
        $inputs = $self->inputs;
    }

    my $job = {inputs => $inputs, query => $query};
    if ($timeout) {
        $job->{$timeout} = $timeout;
    }

    my $content = JSON::encode_json($job);

    my $request =
      $self->client->request('POST', [$self->client->mapred_prefix]);
    $request->content($content);
    my $response = $self->client->useragent->request($request);

    my $result   = JSON::decode_json($response->content);

    my @phases = $self->phases;
    if (ref $phases[-1] ne 'Net::Riak::LinkPhase') {
        return $result;
    }

    my $a = [];
    foreach (@$result) {
        my $l = Net::Riak::Link->new(
            bucket => Net::Riak::Bucket->new(name => $_->[0], client => $self->client),
            key    => $_->[1],
            tag    => $_->[2],
            client => $self->client
        );
        push @$a, $l;
    }
    return $a;
}

1;

=head1 SYNOPSIS

=head1 DESCRIPTION

=head2 ATTRIBUTES

=over 4

=item B<phases>

=item B<inputs_bucket>

=item B<inputs>

=item B<input_mode>

=back

=head2 METHODS

=over 4

=item add

Add inputs to a map/reduce operation. This method takes three different forms, depending on the provided inputs. You can specify either a RiakObject, a string bucket name, or a bucket, key, and additional arg.

=item add_object

=item add_bucket_key_data

=item add_bucket

=item link

=item map

=item reduce

=item run

=back

=cut


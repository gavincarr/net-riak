use Data::Dumper;
use Net::Riak;

my $client = Net::Riak->new(host => 'http://127.0.0.1:8098');

# set up a bucket containing two person/user records and store them
my $bucket_one = $client->bucket('ONE');
my $ref1 = {
    username => 'griffinp',
    fullname => 'Peter Griffin',
    email => 'peter@familyguy.com'
};
my $ref2 = {
    username => 'griffins',
    fullname => 'Stewie Griffin',
    email => 'stewie@familyguy.com'
};
$bucket_one->new_object( $ref1->{username} => $ref1 )->store(1,1);
$bucket_one->new_object( $ref2->{username} => $ref2 )->store(1,1);

# create another bucket to store some data that will link to users
my $bucket_two = $client->bucket('TWO');

# create the object
my $item_data = {
    a_number  => rand(),
    some_text => 'e86d62c91139f328df5f05e9698a248f',
    epoch     => time()
};
my $item = $bucket_two->new_object( '25FCBA57-8D75-41B6-9E5A-0E2528BB3342' => $item_data );

# create a link to each person that is stored in bucket 'ONE' and associate the link
# with the $item object
foreach my $person ( $ref1, $ref2 ) {
    my $link = Net::Riak::Link->new(
        bucket => $bucket_one,
        key    => $person->{username},
        tag    => 'owners'
    );
    $item->add_link( $link );
}

# store to Riak
$item->store( 1, 1 );

# This shows the two links associated with the object
print Dumper( $item );

# this does not show the links
print Dumper( $bucket_two->get('25FCBA57-8D75-41B6-9E5A-0E2528BB3342', [1]) ) ;

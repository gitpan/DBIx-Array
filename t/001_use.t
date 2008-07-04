# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 2;

BEGIN { use_ok( 'DBIx::Array' ); }

my $dbh = DBIx::Array->new ();
isa_ok ($dbh, 'DBIx::Array');

# -*- perl -*-

# t/001_load.t - check module loading and create testing directory

use Test::More tests => 3;

BEGIN { use_ok( 'DBIx::Array' ); }

my $sdb = DBIx::Array->new (name=>"String");
isa_ok ($sdb, 'DBIx::Array');
is($sdb->name, "String", '$sdb->name');

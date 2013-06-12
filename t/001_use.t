# -*- perl -*-
use strict;
use warnings;
use Test::More tests => 6;

BEGIN { use_ok( 'DBIx::Array' ); }
BEGIN { use_ok( 'DBIx::Array::Export' ); }

my $sdb = DBIx::Array->new (name=>"String");
isa_ok ($sdb, 'DBIx::Array');
is($sdb->name, "String", '$sdb->name');

$sdb = DBIx::Array::Export->new (name=>"String");
isa_ok ($sdb, 'DBIx::Array::Export');
is($sdb->name, "String", '$sdb->name');

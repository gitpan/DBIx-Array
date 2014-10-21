# -*- perl -*-
use strict;
use warnings;
use Data::Dumper qw{Dumper};
use Test::More tests => 55 * 2 + 1;

BEGIN { use_ok( 'DBIx::Array' ); }

{
  package #hide from CPAN indexer
    My::Package;
  use base qw{Package::New};
  sub id {shift->{"ID"}};
  sub type {shift->{"TYPE"}};
  sub name {shift->{"NAME"}};
}

my $connection={
                 "DBD::SQLite" => "dbi:SQLite:dbname=:memory",
                 "DBD::CSV"    => "dbi:CSV:f_dir=.",
                 "DBD::XBase"  => "dbi:XBase:.",
               };

foreach my $driver ("DBD::CSV", "DBD::XBase") {
  diag("Driver: $driver");
  my $dba=DBIx::Array->new;
  isa_ok($dba, 'DBIx::Array');
  my $table="dbixarray";
  unlink($table) if -w $table;
  eval "require $driver";
  my $no_driver=$@;
  diag("Found database driver $driver") unless $no_driver;
  my $reason="Database driver $driver not installed";

  SKIP: {
    skip $reason, 3 if $no_driver;

    die("connection not defined for $driver") unless $connection->{$driver};
    $dba->connect($connection->{$driver}, "", "", {RaiseError=>0, AutoCommit=>1});

    #$dba->dbh->do("DROP TABLE $table");
    $dba->dbh->do("CREATE TABLE $table (ID INTEGER,TYPE CHAR(1),NAME VARCHAR(10))");
    is($dba->absinsert($table, {ID=>0, TYPE=>"a", NAME=>"foo"}), 1, 'absinsert');
    is($dba->absinsert($table, {ID=>1, TYPE=>"b", NAME=>"bar"}), 1, 'absinsert');
    is($dba->absinsert($table, {ID=>2, TYPE=>"c", NAME=>"baz"}), 1, 'absinsert');
  }

  SKIP: {
    skip $reason, 22 if $no_driver;
    my $array=$dba->absarrayobject("My::Package", $table, [qw{ID TYPE NAME}], {}, [qw{ID}]);
    isa_ok($array, "ARRAY", 'absarrayhashname scalar context');
    isa_ok($array->[0], "My::Package", 'absarrayobject row 0');
    isa_ok($array->[1], "My::Package", 'absarrayobject row 1');
    isa_ok($array->[2], "My::Package", 'absarrayobject row 2');
    diag(Dumper $array);
    is($array->[0]->{'ID'}, 0, 'data');
    is($array->[0]->{'TYPE'}, "a", 'data');
    is($array->[0]->{'NAME'}, "foo", 'data');
    is($array->[1]->{'ID'}, 1, 'data');
    is($array->[1]->{'TYPE'}, "b", 'data');
    is($array->[1]->{'NAME'}, "bar", 'data');
    is($array->[2]->{'ID'}, 2, 'data');
    is($array->[2]->{'TYPE'}, "c", 'data');
    is($array->[2]->{'NAME'}, "baz", 'data');
    is($array->[0]->id, 0, 'data');
    is($array->[0]->type, "a", 'data');
    is($array->[0]->name, "foo", 'data');
    is($array->[1]->id, 1, 'data');
    is($array->[1]->type, "b", 'data');
    is($array->[1]->name, "bar", 'data');
    is($array->[2]->id, 2, 'data');
    is($array->[2]->type, "c", 'data');
    is($array->[2]->name, "baz", 'data');
  }

  SKIP: {
    skip $reason, 22 if $no_driver;
    my $array=$dba->sqlarrayobject("My::Package", qq{SELECT ID, TYPE, NAME from $table ORDER BY ID});
    isa_ok($array, "ARRAY", 'absarrayhashname scalar context');
    isa_ok($array->[0], "My::Package", 'absarrayobject row 0');
    isa_ok($array->[1], "My::Package", 'absarrayobject row 1');
    isa_ok($array->[2], "My::Package", 'absarrayobject row 2');
    diag(Dumper $array);
    is($array->[0]->{'ID'}, 0, 'data');
    is($array->[0]->{'TYPE'}, "a", 'data');
    is($array->[0]->{'NAME'}, "foo", 'data');
    is($array->[1]->{'ID'}, 1, 'data');
    is($array->[1]->{'TYPE'}, "b", 'data');
    is($array->[1]->{'NAME'}, "bar", 'data');
    is($array->[2]->{'ID'}, 2, 'data');
    is($array->[2]->{'TYPE'}, "c", 'data');
    is($array->[2]->{'NAME'}, "baz", 'data');
    is($array->[0]->id, 0, 'data');
    is($array->[0]->type, "a", 'data');
    is($array->[0]->name, "foo", 'data');
    is($array->[1]->id, 1, 'data');
    is($array->[1]->type, "b", 'data');
    is($array->[1]->name, "bar", 'data');
    is($array->[2]->id, 2, 'data');
    is($array->[2]->type, "c", 'data');
    is($array->[2]->name, "baz", 'data');
  }

  SKIP: {
    skip $reason, 7 if $no_driver;
    my ($object)=$dba->absarrayobject("My::Package", $table, [qw{ID TYPE NAME}], {ID=>0});
    isa_ok($object, "My::Package", 'absarrayobject');
    diag(Dumper $object);
    is($object->{'ID'}, 0, 'data');
    is($object->{'TYPE'}, "a", 'data');
    is($object->{'NAME'}, "foo", 'data');
    is($object->id, 0, 'data');
    is($object->type, "a", 'data');
    is($object->name, "foo", 'data');
  }

  SKIP: {
    skip $reason, 0 if $no_driver;
    $dba->dbh->do("DROP TABLE $table");
  }
}
